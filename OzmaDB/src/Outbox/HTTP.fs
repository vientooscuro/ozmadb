module OzmaDB.Outbox.HTTP

open System
open System.Net.Http
open System.Text
open System.Threading
open System.Threading.Tasks
open FSharpPlus

[<NoComparison; NoEquality>]
type OutboundHttpPolicy =
    { Enabled: bool
      AllowedHosts: string[]
      DefaultTimeoutMs: int
      MaxTimeoutMs: int
      MaxRetries: int
      RetryBaseDelayMs: int
      UserAgent: string }

let defaultOutboundHttpPolicy =
    { Enabled = false
      AllowedHosts = [||]
      DefaultTimeoutMs = 5000
      MaxTimeoutMs = 15000
      MaxRetries = 2
      RetryBaseDelayMs = 200
      UserAgent = "ozmadb/0.0" }

[<NoComparison; NoEquality>]
type HttpDispatchRequest =
    { Method: string
      Url: string
      Headers: Map<string, string>
      Body: string option
      TimeoutMs: int option
      Retries: int option
      RetryBaseDelayMs: int option }

[<NoComparison; NoEquality>]
type HttpDispatchResponse =
    { Status: int
      Url: string
      Headers: Map<string, string[]>
      Body: string }

let private normalizeHostPattern (pattern: string) = pattern.Trim().ToLowerInvariant()

let normalizePolicy (policy: OutboundHttpPolicy) =
    { policy with
        AllowedHosts =
            policy.AllowedHosts
            |> Seq.map normalizeHostPattern
            |> Seq.filter (String.IsNullOrWhiteSpace >> not)
            |> Seq.distinct
            |> Seq.toArray }

let private hostMatchesPattern (host: string) (pattern: string) =
    if pattern = "*" then
        true
    elif pattern.StartsWith("*.") then
        let suffix = pattern.Substring(2)
        host.EndsWith("." + suffix, StringComparison.Ordinal)
    else
        host = pattern

let private isHostAllowed (policy: OutboundHttpPolicy) (host: string) =
    policy.AllowedHosts |> Array.exists (hostMatchesPattern host)

let validateUrlAgainstPolicy (policy: OutboundHttpPolicy) (rawUrl: string) : Result<Uri, string> =
    match Uri.TryCreate(rawUrl, UriKind.Absolute) with
    | (false, _) -> Error(sprintf "Invalid absolute URL: %s" rawUrl)
    | (true, uri) ->
        let scheme = uri.Scheme.ToLowerInvariant()
        let host = uri.Host.ToLowerInvariant()

        if scheme <> "http" && scheme <> "https" then
            Error(sprintf "Only HTTP/HTTPS URLs are allowed: %s" rawUrl)
        elif not (isHostAllowed policy host) then
            Error(sprintf "Host '%s' is not allowed by outbound HTTP policy" host)
        else
            Ok uri

let private isRetriableStatus (status: int) = status = 429 || status >= 500

let private retryDelayMs (attempt: int) (baseDelayMs: int) =
    let pow = pown 2 (max 0 attempt)
    min 30000 (max 1 baseDelayMs * pow)

let private mergeHeaders (response: HttpResponseMessage) : Map<string, string[]> =
    seq {
        for kv in response.Headers do
            yield (kv.Key, kv.Value |> Seq.toArray)

        if not (isNull response.Content) then
            for kv in response.Content.Headers do
                yield (kv.Key, kv.Value |> Seq.toArray)
    }
    |> Map.ofSeq

let private client =
    lazy
        (let handler = new SocketsHttpHandler()
         handler.AllowAutoRedirect <- false

         handler.AutomaticDecompression <-
             System.Net.DecompressionMethods.GZip
             ||| System.Net.DecompressionMethods.Deflate
             ||| System.Net.DecompressionMethods.Brotli

         new HttpClient(handler, false))

let private setHeaders (msg: HttpRequestMessage) (headers: Map<string, string>) =
    for KeyValue(k, v) in headers do
        if String.Equals(k, "content-type", StringComparison.OrdinalIgnoreCase) then
            match msg.Content with
            | null -> ()
            | content ->
                content.Headers.Remove("Content-Type") |> ignore
                content.Headers.TryAddWithoutValidation(k, v) |> ignore
        else if not <| msg.Headers.TryAddWithoutValidation(k, v) then
            match msg.Content with
            | null -> ()
            | content -> content.Headers.TryAddWithoutValidation(k, v) |> ignore

let dispatchHttp
    (policy: OutboundHttpPolicy)
    (req: HttpDispatchRequest)
    (cancellationToken: CancellationToken)
    : Task<HttpDispatchResponse> =
    task {
        let policy = normalizePolicy policy

        if not policy.Enabled then
            failwith "Outbound HTTP is disabled"

        let uri =
            match validateUrlAgainstPolicy policy req.Url with
            | Ok uri -> uri
            | Error err -> failwith err

        let timeoutMs =
            req.TimeoutMs
            |> Option.defaultValue policy.DefaultTimeoutMs
            |> min policy.MaxTimeoutMs
            |> max 1

        let retries =
            req.Retries
            |> Option.defaultValue policy.MaxRetries
            |> min policy.MaxRetries
            |> max 0

        let retryBaseDelayMs =
            req.RetryBaseDelayMs |> Option.defaultValue policy.RetryBaseDelayMs |> max 1

        let methodStr =
            match String.IsNullOrWhiteSpace(req.Method) with
            | true -> "GET"
            | false -> req.Method.Trim().ToUpperInvariant()

        let mutable attempt = 0
        let mutable lastError = None: exn option
        let mutable lastResponse = None: HttpDispatchResponse option
        let mutable shouldContinue = true

        while shouldContinue
              && attempt <= retries
              && not cancellationToken.IsCancellationRequested do
            use requestMessage = new HttpRequestMessage(HttpMethod(methodStr), uri)

            match req.Body with
            | Some body -> requestMessage.Content <- new StringContent(body, Encoding.UTF8)
            | None -> ()

            setHeaders requestMessage req.Headers
            requestMessage.Headers.UserAgent.ParseAdd(policy.UserAgent)

            try
                use timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                timeoutCts.CancelAfter(timeoutMs)

                use! response =
                    client.Value.SendAsync(requestMessage, HttpCompletionOption.ResponseHeadersRead, timeoutCts.Token)

                let! body = response.Content.ReadAsStringAsync(timeoutCts.Token)

                let parsedResponse =
                    { Status = int response.StatusCode
                      Url = string uri
                      Headers = mergeHeaders response
                      Body = body }

                lastResponse <- Some parsedResponse

                if isRetriableStatus parsedResponse.Status && attempt < retries then
                    let delayMs = retryDelayMs attempt retryBaseDelayMs
                    attempt <- attempt + 1
                    do! Task.Delay(delayMs, cancellationToken)
                else
                    shouldContinue <- false
            with
            | :? OperationCanceledException as e when not cancellationToken.IsCancellationRequested && attempt < retries ->
                lastError <- Some e
                let delayMs = retryDelayMs attempt retryBaseDelayMs
                attempt <- attempt + 1
                do! Task.Delay(delayMs, cancellationToken)
            | :? HttpRequestException as e when attempt < retries ->
                lastError <- Some e
                let delayMs = retryDelayMs attempt retryBaseDelayMs
                attempt <- attempt + 1
                do! Task.Delay(delayMs, cancellationToken)
            | e ->
                lastError <- Some e
                shouldContinue <- false

        match lastResponse, lastError with
        | Some response, _ -> return response
        | None, Some e -> return raise e
        | None, None -> return failwith "Failed to dispatch HTTP request"
    }
