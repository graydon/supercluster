// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

module StellarJobExec

open k8s
open k8s.Models
open Logging
open StellarDestination
open StellarDataDump
open StellarFormation
open StellarKubeSpecs
open System
open System.Threading
open Microsoft.Rest

type JobStatusTable() =
    let mutable running = Set.empty
    let mutable finished = Map.empty
    member self.NoteRunning(name: string) = running <- running.Add(name)

    member self.NoteFinished (name: string) (ok: bool) =
        running <- running.Remove(name)
        finished <- finished.Add(name, ok)

    member self.IsFinished(name: string) : bool = finished.ContainsKey name
    member self.IsRunning(name: string) : bool = running.Contains name
    member self.NumRunning() : int = running.Count
    member self.GetFinishedTable() : Map<string, bool> = finished

type StellarFormation with
    member self.StartJob(j: V1Job) : V1Job =
        try
            let ns = self.NetworkCfg.NamespaceProperty
            self.sleepUntilNextRateLimitedApiCallTime ()
            let j = self.Kube.CreateNamespacedJob(body = j, namespaceParameter = ns)
            self.NamespaceContent.Add(j)
            j
        with :? HttpOperationException as w ->
            LogError "err: %s" w.Message
            LogError "err: %s" w.Response.Content
            LogError "err: %s" w.Response.ReasonPhrase
            reraise ()

    member self.CheckJob (j: V1Job) (jst: JobStatusTable) (destination: Destination) =
        let name = j.Metadata.Name
        assert (not (jst.IsFinished(name)))
        let ns = j.Metadata.NamespaceProperty
        let jobIsCompleted = j.Status.CompletionTime.HasValue
        let jobActivePodCount = j.Status.Active.GetValueOrDefault(0)
        let jobFailedPodCount = j.Status.Failed.GetValueOrDefault(0)
        let jobSucceededPodCount = j.Status.Succeeded.GetValueOrDefault(0)

        // Check if any containers were terminated for a non-OOM reason. We fail on the first non-OOM failure
        // seen, but we allow two failures if they are due to an OOM kill.

        let checkNonOomFail () : bool =
            let mutable isNonOomFail = false

            if jobFailedPodCount > 0 then
                self.sleepUntilNextRateLimitedApiCallTime ()

                let pods =
                    self.Kube.ListNamespacedPod(namespaceParameter = ns, labelSelector = "job-name=" + name)

                for pod in pods.Items do
                    if pod.Status.ContainerStatuses <> null then
                        for status in pod.Status.ContainerStatuses do
                            if status.State <> null
                               && status.State.Terminated <> null
                               && status.State.Terminated.ExitCode <> 0 // Success
                               && status.State.Terminated.ExitCode <> 137 // OOM
                            then
                                isNonOomFail <- true

            isNonOomFail

        if (jobIsCompleted && jobActivePodCount = 0)
           || jobFailedPodCount > 2
           || checkNonOomFail () then
            LogInfo "Finished job %s: %d fail / %d success" name jobFailedPodCount jobSucceededPodCount
            let ok = (jobSucceededPodCount = 1)
            jst.NoteFinished name ok

            if ok then
                LogInfo "Job %s passed" name
            else
                self.LogState j
                failwith ("Job " + name + " failed")

            try
                self.FinishJob destination j
            with e ->
                LogError "Error occurred during cleanup of job %s" name
                raise e

            LogInfo "Finished cleaning up after job %s" name

    member self.RunSingleJob
        (destination: Destination)
        (job: (string array))
        (image: string)
        (useConfigFile: bool)
        : Map<string, bool> =
        self.RunSingleJobWithTimeout destination None job image useConfigFile

    member self.RunSingleJobWithTimeout
        (destination: Destination)
        (timeout: TimeSpan option)
        (cmd: (string array))
        (image: string)
        (useConfigFile: bool)
        : Map<string, bool> =
        let startTime = DateTime.UtcNow
        let jst = new JobStatusTable()
        let j = self.StartJobForCmd cmd image useConfigFile
        let name = j.Metadata.Name
        let ns = j.Metadata.NamespaceProperty
        jst.NoteRunning j.Metadata.Name

        while not (jst.IsFinished(name)) do
            if timeout.IsSome && (DateTime.UtcNow - startTime) > timeout.Value then
                let err =
                    (sprintf "Timeout while waiting %O for job '%s'" (timeout.Value) (String.Join(" ", cmd)))

                LogError "%s" err
                failwith err

            // sleep for 30 seconds
            self.sleepUntilNextRateLimitedApiCallTime ()

            let js = self.Kube.ReadNamespacedJob(name = name, namespaceParameter = ns)
            self.CheckJob js jst destination
            Thread.Sleep(30000)

        let endTime = DateTime.UtcNow
        LogInfo "Job finished after %O: '%s'" (endTime - startTime) (String.Join(" ", cmd))
        jst.GetFinishedTable()

    member self.RunParallelJobsInRandomOrder
        (parallelism: int)
        (destination: Destination)
        (allJobs: ((string array) array))
        (image: string)
        : Map<string, bool> =
        let jobArr = Array.copy allJobs

        let shuffle (arr: 'a array) =
            let rng = System.Random()
            let rnd _ = rng.Next(arr.Length)

            let swap i j =
                let tmp = arr.[i]
                arr.[i] <- arr.[j]
                arr.[j] <- tmp

            Array.iteri (fun i _ -> swap i (rnd ())) arr

        shuffle jobArr
        let mutable jobQueue = Array.toList jobArr

        self.RunParallelJobs
            parallelism
            destination
            (fun _ ->
                (match jobQueue with
                 | [] -> None
                 | head :: tail ->
                     jobQueue <- tail
                     Some head))
            image

    member self.RunParallelJobs
        (parallelism: int)
        (destination: Destination)
        (nextJob: (unit -> (string array) option))
        (image: string)
        : Map<string, bool> =
        let jst = new JobStatusTable()
        let mutable moreJobs = true
        // We check to see if there are pods that have been in "Pending"
        // state for more than 120 minutes. This typically means the cluster
        // is low on fixed resources and isn't actually going to be able to
        // run our parallel-job-set to completion, so we prefer to fail early
        // in this case.
        let podBuildupTimeoutMinutes = 120
        let podBuildupCheckMinutes = 3
        let mutable lastPodBuildupCheckTime = DateTime.UtcNow

        let checkPendingPodBuildup () : unit =
            let now = DateTime.UtcNow

            if now.Subtract(lastPodBuildupCheckTime).Minutes >= podBuildupCheckMinutes then
                lastPodBuildupCheckTime <- now
                let ns = self.NetworkCfg.NamespaceProperty
                LogInfo "Checking for pod buildup"
                let pods = self.Kube.ListNamespacedPod(namespaceParameter = ns)

                for pod in pods.Items do
                    let stillPending = (pod.Status.Phase = "Pending")

                    let notReady =
                        (pod.Status.ContainerStatuses.Count > 0
                         && pod.Status.ContainerStatuses.[0].Ready = false)

                    let waitingTooLong =
                        (pod.Metadata.CreationTimestamp.HasValue
                         && now.Subtract(pod.Metadata.CreationTimestamp.Value).Minutes > podBuildupTimeoutMinutes)

                    if (stillPending || notReady) && waitingTooLong then
                        failwith (
                            sprintf
                                "Pod '%s' has been 'Pending' for more than %d minutes, cluster resources likely exhausted"
                                pod.Metadata.Name
                                podBuildupTimeoutMinutes
                        )
                    else
                        ()

                LogInfo "Did not find pod buildup"

        let addJob () : unit =
            match nextJob () with
            | None -> moreJobs <- false
            | Some (cmd) ->
                let j = self.StartJobForCmd cmd image true
                jst.NoteRunning j.Metadata.Name
                LogInfo "Adding job %s (numRunning = %d)" j.Metadata.Name (jst.NumRunning())

        while moreJobs || jst.NumRunning() > 0 do
            checkPendingPodBuildup ()
            let mutable jobCount = 0
            // check for completed and move to finished from running
            self.sleepUntilNextRateLimitedApiCallTime ()

            let jobs =
                self.Kube.ListNamespacedJob(namespaceParameter = self.NetworkCfg.NamespaceProperty)

            for job in jobs.Items do
                if jst.IsRunning(job.Metadata.Name) then
                    self.CheckJob job jst destination
                    jobCount <- jobCount + 1

            // We remove from the running set before deleting the job, so the
            // only way this condition can be true is if something other than
            // supercluster deletes jobs started by this run
            if jst.NumRunning() > jobCount then
                failwith (
                    sprintf "NumRunning (%d) is greater than number of jobs seen (%d)" (jst.NumRunning()) jobCount
                )

            while jst.NumRunning() < parallelism && moreJobs do
                addJob ()

            // sleep for one minute
            Thread.Sleep(60000)

        LogInfo "Finished parallel-job loop"

        // make sure we're actually done
        assert (nextJob () = None)
        assert (jst.NumRunning() = 0)
        jst.GetFinishedTable()

    member self.LogState(j: V1Job) =
        let jobName = j.Metadata.Name
        let ns = j.Metadata.NamespaceProperty
        let mutable message = new System.Text.StringBuilder()
        let addMsg (s: string) = message.Append(s) |> ignore
        self.sleepUntilNextRateLimitedApiCallTime ()

        let pods =
            self.Kube.ListNamespacedPod(namespaceParameter = ns, labelSelector = "job-name=" + jobName)

        for pod in pods.Items do
            addMsg (sprintf "Pod %s failed, HostIp=%s" pod.Metadata.Name pod.Status.HostIP)
            let podName = pod.Metadata.Name

            // Container errors
            if pod.Status.ContainerStatuses <> null then
                for status in pod.Status.ContainerStatuses do
                    if status.State <> null
                       && status.State.Terminated <> null
                       && status.State.Terminated.ExitCode <> 0 // Success
                    then
                        addMsg (
                            sprintf
                                "Container %s terminated. ExitCode = %d, Reason = %s"
                                status.Name
                                status.State.Terminated.ExitCode
                                status.State.Terminated.Reason
                        )

            // Pod events
            let fs = sprintf "involvedObject.name=%s" podName
            self.sleepUntilNextRateLimitedApiCallTime ()
            let events = self.Kube.ListNamespacedEvent(namespaceParameter = ns, fieldSelector = fs)

            for ev in events.Items do
                if ev.Reason <> "DNSConfigForming" && ev.Type <> "Normal" then
                    addMsg (
                        sprintf "Pod Event %s - Type=%s, Reason=%s, Message=%s" podName ev.Type ev.Reason ev.Message
                    )

        // Job events
        let fs = sprintf "involvedObject.name=%s" jobName
        self.sleepUntilNextRateLimitedApiCallTime ()
        let events = self.Kube.ListNamespacedEvent(namespaceParameter = ns, fieldSelector = fs)

        for ev in events.Items do
            if ev.Reason <> "DNSConfigForming" && ev.Type <> "Normal" then
                addMsg (sprintf "Job Event %s - Type=%s, Reason=%s, Message=%s" jobName ev.Type ev.Reason ev.Message)

        LogWarn "%s" (message.ToString())

    member self.CheckAllJobsSucceeded(jobs: Map<string, bool>) =
        let anyBad = ref false

        Map.iter
            (fun k v ->
                if v then
                    LogInfo "Job %s passed" k
                else
                    (LogError "Job %s failed" k
                     anyBad := true))
            jobs

        if !anyBad then failwith "One of more jobs failed"

    member self.StartJobForCmd (cmd: string array) (image: string) (useConfigFile: bool) : V1Job =
        let jobNum = self.NextJobNum
        self.StartJob(self.NetworkCfg.GetJobFor jobNum cmd image useConfigFile)

    member self.FinishJob (destination: Destination) (j: V1Job) : unit =
        // We need to dump the job logs as we go and mop up the jobs
        // because the namespace we're running within has a limited
        // quota for number of jobs / pods available and a big parallel
        // catchup will exhaust that set.
        self.DumpJobLogs destination j.Metadata.Name
        self.NamespaceContent.Del(j)
