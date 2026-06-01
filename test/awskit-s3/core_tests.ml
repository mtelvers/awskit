open Awskit_s3

let test_time = Ptime.of_date_time ((2026, 4, 8), ((12, 0, 0), 0)) |> Option.get

let creds =
  Credentials.create_exn ~access_key_id:"AKID" ~secret_access_key:"SECRET" ()

let error_t = Alcotest.testable Error.pp Error.equal

let ok_or_fail label = function
  | Ok value -> value
  | Error error -> Alcotest.failf "%s: %a" label Error.pp error

let header name headers = List.assoc_opt name headers

let checksum_value = function
  | None -> None
  | Some (checksum : Object.Checksum.response) -> Some checksum.value

let version_string = Option.map Object.Version_id.to_string
let query_param name url = Uri.query (Uri.of_string url) |> List.assoc_opt name

let check_checksum label algorithm value = function
  | None -> Alcotest.failf "%s: expected checksum" label
  | Some (checksum : Object.Checksum.response) ->
      Alcotest.(check bool)
        (label ^ " algorithm") true
        (checksum.algorithm = algorithm);
      Alcotest.(check string) (label ^ " value") value checksum.value

let test_endpoint_resolution () =
  let result =
    Presigned.get_object
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"my-bucket" ~key:"dir/file.txt"
      ()
    |> ok_or_fail "presigned default endpoint"
  in
  Alcotest.(check bool)
    "virtual-hosted default" true
    (String.starts_with
       ~prefix:"https://my-bucket.s3.us-east-1.amazonaws.com/dir/file.txt"
       result.url);
  let dotted =
    Presigned.get_object
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"my.bucket" ~key:"file.txt" ()
    |> ok_or_fail "presigned dotted bucket"
  in
  Alcotest.(check bool)
    "dotted bucket path-style" true
    (String.starts_with
       ~prefix:"https://s3.us-east-1.amazonaws.com/my.bucket/file.txt"
       dotted.url);
  let endpoint = Endpoint.http_exn ~host:"localhost" ~port:9000 () in
  let result =
    Presigned.get_object
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~endpoint ~addressing_style:`Path
      ~bucket:"my-bucket" ~key:"dir/file.txt" ()
    |> ok_or_fail "presigned endpoint override"
  in
  Alcotest.(check bool)
    "endpoint override path-style" true
    (String.starts_with ~prefix:"http://localhost:9000/my-bucket/dir/file.txt"
       result.url)

let test_endpoint_variants () =
  let dualstack =
    Presigned.get_object
      ~region:(Region.of_string_exn "eu-west-1")
      ~credentials:creds ~now:test_time ~endpoint_variant:`Dualstack
      ~bucket:"bucket" ~key:"file.txt" ()
    |> ok_or_fail "dualstack endpoint"
  in
  Alcotest.(check bool)
    "dualstack" true
    (String.starts_with
       ~prefix:"https://bucket.s3.dualstack.eu-west-1.amazonaws.com/file.txt"
       dualstack.url);
  let accelerate =
    Presigned.get_object
      ~region:(Region.of_string_exn "us-west-2")
      ~credentials:creds ~now:test_time ~endpoint_variant:`Accelerate_dualstack
      ~bucket:"bucket" ~key:"file.txt" ()
    |> ok_or_fail "accelerate endpoint"
  in
  Alcotest.(check bool)
    "accelerate" true
    (String.starts_with
       ~prefix:"https://bucket.s3-accelerate.dualstack.amazonaws.com/file.txt"
       accelerate.url)

let service_error ?code ?message status =
  Awskit.Error.service
    {
      status;
      code;
      message;
      request_id = None;
      host_id = None;
      headers = [];
      body = None;
    }

let test_error_classifiers () =
  let precondition = service_error ~code:"PreconditionFailed" 412 in
  let conditional_conflict =
    service_error ~code:"ConditionalRequestConflict" 409
  in
  let generic_conflict = service_error 409 in
  Alcotest.(check bool)
    "precondition failed" true
    (Error.is_precondition_failed precondition);
  Alcotest.(check bool)
    "conditional request conflict by code" true
    (Error.is_conditional_request_conflict conditional_conflict);
  Alcotest.(check bool)
    "generic 409 is not conditional conflict" false
    (Error.is_conditional_request_conflict generic_conflict);
  Alcotest.(check bool)
    "conditional failure includes precondition" true
    (Error.is_conditional_failure precondition);
  Alcotest.(check bool)
    "conditional failure includes conflict" true
    (Error.is_conditional_failure conditional_conflict)

let test_presigned_result () =
  let result =
    Presigned.get_object
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"bucket" ~key:"file.txt" ()
    |> ok_or_fail "presigned get"
  in
  Alcotest.(check string)
    "method" "GET"
    (Awskit.Request.Method.to_string
       (result.method_ :> Awskit.Request.Method.t));
  Alcotest.(check bool)
    "has signature" true
    (String.contains result.url '?'
    && String.contains result.url '='
    && String.contains result.url '&');
  Alcotest.(check bool)
    "virtual-hosted URL" true
    (String.starts_with ~prefix:"https://bucket.s3.us-east-1.amazonaws.com/"
       result.url)

let test_presigned_put_checksum_headers () =
  let checksum : Object.Checksum.request =
    { Object.Checksum.algorithm = `SHA1; value = Some "provided-sha1" }
  in
  let options =
    { Presigned.Put_object.default_options with checksum = Some checksum }
  in
  let result =
    Presigned.put_object
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"bucket" ~key:"file.txt"
      ~options ()
    |> ok_or_fail "presigned put"
  in
  Alcotest.(check (option string))
    "checksum algorithm header" (Some "SHA1")
    (header "x-amz-checksum-algorithm" result.headers);
  Alcotest.(check (option string))
    "checksum value header" (Some "provided-sha1")
    (header "x-amz-checksum-sha1" result.headers);
  let signed_headers =
    match query_param "X-Amz-SignedHeaders" result.url with
    | Some [ value ] -> String.split_on_char ';' value
    | _ -> Alcotest.fail "missing signed headers"
  in
  Alcotest.(check bool)
    "signed checksum algorithm" true
    (List.mem "x-amz-checksum-algorithm" signed_headers);
  Alcotest.(check bool)
    "signed checksum value" true
    (List.mem "x-amz-checksum-sha1" signed_headers)

let test_presigned_upload_part () =
  let upload_id = Multipart.Upload_id.of_string_exn "upload-1" in
  let checksum : Object.Checksum.request =
    { Object.Checksum.algorithm = `SHA256; value = Some "provided-sha256" }
  in
  let options =
    { Presigned.Upload_part.default_options with checksum = Some checksum }
  in
  let result =
    Presigned.upload_part
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"bucket" ~key:"large.bin"
      ~upload_id ~part_number:7 ~options ()
    |> ok_or_fail "presigned upload part"
  in
  Alcotest.(check string)
    "method" "PUT"
    (Awskit.Request.Method.to_string
       (result.method_ :> Awskit.Request.Method.t));
  Alcotest.(check (option (list string)))
    "part number" (Some [ "7" ])
    (query_param "partNumber" result.url);
  Alcotest.(check (option (list string)))
    "upload id" (Some [ "upload-1" ])
    (query_param "uploadId" result.url);
  Alcotest.(check (option string))
    "checksum algorithm header" (Some "SHA256")
    (header "x-amz-checksum-algorithm" result.headers);
  Alcotest.(check (option string))
    "checksum value header" (Some "provided-sha256")
    (header "x-amz-checksum-sha256" result.headers);
  let signed_headers =
    match query_param "X-Amz-SignedHeaders" result.url with
    | Some [ value ] -> String.split_on_char ';' value
    | _ -> Alcotest.fail "missing signed headers"
  in
  Alcotest.(check bool)
    "signed checksum algorithm" true
    (List.mem "x-amz-checksum-algorithm" signed_headers);
  Alcotest.(check bool)
    "signed checksum value" true
    (List.mem "x-amz-checksum-sha256" signed_headers);
  match
    Presigned.upload_part
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"bucket" ~key:"large.bin"
      ~upload_id ~part_number:0 ()
  with
  | Error _ -> ()
  | Ok _ -> Alcotest.fail "expected invalid part number"

let test_presigned_rejects_header_newline () =
  let options =
    {
      Presigned.Put_object.default_options with
      headers = [ ("x-test", "ok\r\nInjected: yes") ];
    }
  in
  match
    Presigned.put_object
      ~region:(Region.of_string_exn "us-east-1")
      ~credentials:creds ~now:test_time ~bucket:"bucket" ~key:"file.txt"
      ~options ()
  with
  | Error (Awskit.Error.Validation _) -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected header validation error"

module Recording_runtime = struct
  type response = {
    status : int;
    headers : (string * string) list;
    body : string;
    read_error_after : int option;
  }

  type call = { request : Awskit.Request.t; body : string }

  type upload_body = {
    body : (string, Awskit.Error.t) result;
    descriptor : Awskit.Body.Upload.descriptor;
  }

  type connection = {
    region : Region.t;
    credentials : Credentials.t;
    endpoint_config : Awskit_s3.endpoint_config;
    retry_policy : Awskit.Retry.t;
    mutable calls : call list;
    mutable responses : response list;
    mutable sleeps : Ptime.Span.t list;
    mutable discarded : string list;
  }

  type 'a t = 'a

  let return value = value
  let bind value f = f value

  type download_body = { body : string; read_error_after : int option }
  type upload_writer = Buffer.t

  type download_reader = {
    body : string;
    read_error_after : int option;
    mutable offset : int;
  }

  let connect ?(endpoint_config = Awskit_s3.default_endpoint_config)
      ?(region = Region.of_string_exn "us-east-1")
      ?(retry_policy = Awskit.Retry.default) responses =
    {
      region;
      credentials = creds;
      endpoint_config;
      retry_policy;
      calls = [];
      responses;
      sleeps = [];
      discarded = [];
    }

  let last_call conn =
    match conn.calls with
    | call :: _ -> call
    | [] -> Alcotest.fail "expected recorded request"

  let now _ = test_time
  let region conn = conn.region
  let credentials conn = Ok conn.credentials
  let s3_endpoint_config conn = conn.endpoint_config
  let retry_policy conn = conn.retry_policy
  let sleep conn span = conn.sleeps <- span :: conn.sleeps
  let endpoint _ = None

  let descriptor ?(replayable = true) body =
    {
      Awskit.Body.Upload.content_length =
        Some (Int64.of_int (String.length body));
      payload_hash = Awskit.Body.Payload_hash.sha256_of_string body;
      replayable;
    }

  let upload_body ?replayable body =
    { body = Ok body; descriptor = descriptor ?replayable body }

  let empty_body = upload_body ""
  let string_body body = upload_body body
  let bytes_body body = upload_body (Bytes.to_string body)

  let stream_body descriptor ~write =
    let buffer = Buffer.create 128 in
    let body =
      match write buffer with
      | Ok () -> (
          let body = Buffer.contents buffer in
          let length = Int64.of_int (String.length body) in
          match descriptor.Awskit.Body.Upload.content_length with
          | Some declared when not (Stdlib.Int64.equal declared length) ->
              Error (Awskit.Error.body "upload body length mismatch")
          | _ -> Ok body)
      | Error _ as error -> error
    in
    { body; descriptor }

  let upload_descriptor body = body.descriptor

  let write_string writer body =
    Buffer.add_string writer body;
    Ok ()

  let read reader bytes ~off ~len =
    if len = 0 then Ok 0
    else if
      match reader.read_error_after with
      | Some limit -> reader.offset >= limit
      | None -> false
    then Stdlib.Error (Awskit.Error.body "simulated download read failure")
    else
      let remaining = String.length reader.body - reader.offset in
      if remaining <= 0 then Ok 0
      else
        let copied = min len remaining in
        String.blit reader.body reader.offset bytes off copied;
        reader.offset <- reader.offset + copied;
        Ok copied

  let rec drain reader =
    let bytes = Bytes.create 8 in
    match read reader bytes ~off:0 ~len:(Bytes.length bytes) with
    | Error _ as error -> error
    | Ok 0 -> Ok ()
    | Ok _ -> drain reader

  let with_download_body (body : download_body) ~consume =
    let reader =
      { body = body.body; read_error_after = body.read_error_after; offset = 0 }
    in
    match consume reader with
    | Ok _ as result -> (
        match drain reader with Ok () -> result | Error _ as error -> error)
    | Error _ as error -> (
        match drain reader with
        | Ok () -> error
        | Error _ as drain_error -> drain_error)

  let discard_download_body (body : download_body) =
    let reader =
      { body = body.body; read_error_after = body.read_error_after; offset = 0 }
    in
    let result = drain reader in
    result

  let with_response conn request (body : upload_body) ~f =
    match body.body with
    | Error _ as error -> error
    | Ok body -> (
        conn.calls <- { request; body } :: conn.calls;
        match conn.responses with
        | [] ->
            Error (Awskit.Error.transport ~retryable:false "no canned response")
        | response :: rest ->
            conn.responses <- rest;
            f
              (Awskit.Response.create_exn ~status:response.status
                 ~headers:response.headers ())
              {
                body = response.body;
                read_error_after = response.read_error_after;
              })
end

module Recording_s3 = Awskit_s3.Make (Recording_runtime)

let response ?(headers = []) ?read_error_after status body =
  { Recording_runtime.status; headers; body; read_error_after }

let test_bucket_head_request () =
  let conn =
    Recording_runtime.connect
      [ response 200 ~headers:[ ("x-amz-bucket-region", "us-west-2") ] "" ]
  in
  let info =
    Recording_s3.Bucket.head conn ~bucket:"my-bucket"
    |> ok_or_fail "bucket head"
  in
  Alcotest.(check (option string))
    "region" (Some "us-west-2")
    (Option.map Awskit.Region.to_string info.region);
  let call = Recording_runtime.last_call conn in
  Alcotest.(check string)
    "host" "my-bucket.s3.us-east-1.amazonaws.com" call.request.target.host;
  Alcotest.(check string) "path" "/" call.request.target.path

let test_bucket_list_parse () =
  let body =
    {|<ListAllMyBucketsResult><Buckets><Bucket><Name>alpha</Name><CreationDate>2026-04-08T12:00:00Z</CreationDate></Bucket><Bucket><Name>zeta</Name></Bucket></Buckets></ListAllMyBucketsResult>|}
  in
  let conn = Recording_runtime.connect [ response 200 body ] in
  let buckets = Recording_s3.Bucket.list conn |> ok_or_fail "bucket list" in
  Alcotest.(check (list string))
    "bucket names" [ "alpha"; "zeta" ]
    (List.map (fun (bucket : Bucket.info) -> bucket.name) buckets);
  let call = Recording_runtime.last_call conn in
  Alcotest.(check string)
    "root host" "s3.us-east-1.amazonaws.com" call.request.target.host

let test_bucket_config_parse () =
  let versioning =
    {|<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>|}
  in
  let tagging =
    {|<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>|}
  in
  let encryption =
    {|<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>key-1</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>|}
  in
  let cors =
    {|<CORSConfiguration><CORSRule><ID>web</ID><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedHeader>*</AllowedHeader><ExposeHeader>etag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>|}
  in
  let website =
    {|<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument></WebsiteConfiguration>|}
  in
  let public_access_block =
    {|<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>|}
  in
  let ownership =
    {|<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>|}
  in
  let request_payment =
    {|<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>|}
  in
  let accelerate =
    {|<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>|}
  in
  let policy_status =
    {|<PolicyStatus><IsPublic>false</IsPublic></PolicyStatus>|}
  in
  let logging =
    {|<BucketLoggingStatus><LoggingEnabled><TargetBucket>log-bucket</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>|}
  in
  let conn =
    Recording_runtime.connect
      [
        response 200 versioning;
        response 200 tagging;
        response 200 encryption;
        response 200 cors;
        response 200 website;
        response 200 public_access_block;
        response 200 ownership;
        response 200 request_payment;
        response 200 accelerate;
        response 200 policy_status;
        response 200 logging;
      ]
  in
  let versioning =
    Recording_s3.Bucket.Versioning.get conn ~bucket:"my-bucket"
    |> ok_or_fail "versioning"
  in
  Alcotest.(check bool)
    "versioning enabled" true
    (versioning.status = Some Bucket.Versioning.Status.Enabled);
  let tagging =
    Recording_s3.Bucket.Tagging.get conn ~bucket:"my-bucket"
    |> ok_or_fail "tagging"
  in
  Alcotest.(check int) "tag count" 1 (List.length tagging.tags);
  let encryption =
    Recording_s3.Bucket.Encryption.get conn ~bucket:"my-bucket"
    |> ok_or_fail "encryption"
  in
  (match encryption.config.rules with
  | [ rule ] ->
      Alcotest.(check string)
        "algorithm" "aws:kms"
        (Bucket.Encryption.Algorithm.to_string rule.sse_algorithm);
      Alcotest.(check (option string))
        "kms key" (Some "key-1") rule.kms_master_key_id
  | _ -> Alcotest.fail "expected one encryption rule");
  let cors =
    Recording_s3.Bucket.Cors.get conn ~bucket:"my-bucket" |> ok_or_fail "cors"
  in
  Alcotest.(check int) "cors rule count" 1 (List.length cors.config.rules);
  let website =
    Recording_s3.Bucket.Website.get conn ~bucket:"my-bucket"
    |> ok_or_fail "website"
  in
  Alcotest.(check (option string))
    "index" (Some "index.html") website.config.index_document_suffix;
  let public_access_block =
    Recording_s3.Bucket.Public_access_block.get conn ~bucket:"my-bucket"
    |> ok_or_fail "public access block"
  in
  Alcotest.(check bool)
    "block public policy" true public_access_block.config.block_public_policy;
  let ownership =
    Recording_s3.Bucket.Ownership_controls.get conn ~bucket:"my-bucket"
    |> ok_or_fail "ownership"
  in
  Alcotest.(check string)
    "ownership" "BucketOwnerEnforced"
    (Bucket.Ownership_controls.Object_ownership.to_string
       ownership.config.object_ownership);
  let request_payment =
    Recording_s3.Bucket.Request_payment.get conn ~bucket:"my-bucket"
    |> ok_or_fail "request payment"
  in
  Alcotest.(check bool)
    "requester payer" true
    (request_payment.payer = Some Bucket.Request_payment.Payer.Requester);
  let accelerate =
    Recording_s3.Bucket.Accelerate.get conn ~bucket:"my-bucket"
    |> ok_or_fail "accelerate"
  in
  Alcotest.(check bool)
    "accelerate enabled" true
    (accelerate.status = Some Bucket.Accelerate.Status.Enabled);
  let policy_status =
    Recording_s3.Bucket.Policy_status.get conn ~bucket:"my-bucket"
    |> ok_or_fail "policy status"
  in
  Alcotest.(check (option bool))
    "policy status" (Some false) policy_status.is_public;
  let logging =
    Recording_s3.Bucket.Logging.get conn ~bucket:"my-bucket"
    |> ok_or_fail "logging"
  in
  match logging.config.logging with
  | Some target ->
      Alcotest.(check string) "logging target" "log-bucket" target.target_bucket;
      Alcotest.(check string) "logging prefix" "logs/" target.target_prefix
  | None -> Alcotest.fail "expected logging target"

let test_object_checksum_headers_and_response () =
  let conn =
    Recording_runtime.connect
      [
        response 200
          ~headers:
            [
              ("etag", "\"etag\""); ("x-amz-checksum-sha256", "provided-sha256");
            ]
          "";
      ]
  in
  let checksum : Object.Checksum.request =
    { Object.Checksum.algorithm = `SHA256; value = Some "provided-sha256" }
  in
  let options = { Object.Put.default_options with checksum = Some checksum } in
  let put =
    Recording_s3.Object.Buffer.put_string conn ~bucket:"my-bucket" ~key:"file"
      ~options "hello"
    |> ok_or_fail "put checksum"
  in
  check_checksum "put response checksum" `SHA256 "provided-sha256" put.checksum;
  let call = Recording_runtime.last_call conn in
  Alcotest.(check string) "body" "hello" call.body;
  Alcotest.(check (option string))
    "checksum algorithm header" (Some "SHA256")
    (header "x-amz-checksum-algorithm" call.request.headers);
  Alcotest.(check (option string))
    "checksum value header" (Some "provided-sha256")
    (header "x-amz-checksum-sha256" call.request.headers)

let test_object_precondition_headers () =
  let time = Ptime.to_rfc3339 test_time in
  let conn =
    Recording_runtime.connect
      [
        response 200 ~headers:[ ("etag", "\"put\"") ] "";
        response 200
          ~headers:[ ("etag", "\"get\""); ("content-length", "0") ]
          "";
        response 200
          ~headers:[ ("etag", "\"head\""); ("content-length", "0") ]
          "";
        response 204 "";
        response 200
          {|<CopyObjectResult><ETag>"copy"</ETag></CopyObjectResult>|};
      ]
  in
  let etag = Object.Etag.of_string_exn "\"etag\"" in
  let write_preconditions =
    {
      Object.Preconditions.Write.if_match =
        Some (Object.Etag_condition.etag etag);
      if_none_match = Some Object.Etag_condition.any;
    }
  in
  let put_options =
    { Object.Put.default_options with preconditions = write_preconditions }
  in
  ignore
    (Recording_s3.Object.Buffer.put_string conn ~bucket:"my-bucket" ~key:"file"
       ~options:put_options "body"
    |> ok_or_fail "put preconditions");
  let read_preconditions =
    {
      Object.Preconditions.Read.if_match =
        Some (Object.Etag_condition.etag etag);
      if_none_match = Some Object.Etag_condition.any;
      if_modified_since = Some test_time;
      if_unmodified_since = Some test_time;
    }
  in
  let get_options =
    { Object.Get.default_options with preconditions = read_preconditions }
  in
  ignore
    (Recording_s3.Object.Buffer.get_string conn ~bucket:"my-bucket" ~key:"file"
       ~options:get_options ~max_size:16L ()
    |> ok_or_fail "get preconditions");
  let head_options =
    { Object.Head.default_options with preconditions = read_preconditions }
  in
  ignore
    (Recording_s3.Object.head conn ~bucket:"my-bucket" ~key:"file"
       ~options:head_options ()
    |> ok_or_fail "head preconditions");
  let delete_preconditions =
    {
      Object.Preconditions.Delete.if_match =
        Some (Object.Etag_condition.etag etag);
      if_match_last_modified_time = Some test_time;
      if_match_size = Some 4L;
    }
  in
  let delete_options =
    { Object.Delete.default_options with preconditions = delete_preconditions }
  in
  ignore
    (Recording_s3.Object.delete conn ~bucket:"my-bucket" ~key:"file"
       ~options:delete_options ()
    |> ok_or_fail "delete preconditions");
  let copy_preconditions =
    {
      Object.Preconditions.Copy_source.if_match =
        Some (Object.Etag_condition.etag etag);
      if_none_match = Some Object.Etag_condition.any;
      if_modified_since = Some test_time;
      if_unmodified_since = Some test_time;
    }
  in
  let copy_options =
    {
      Object.Copy.default_options with
      source_preconditions = copy_preconditions;
    }
  in
  ignore
    (Recording_s3.Object.copy conn ~src_bucket:"my-bucket" ~src_key:"file"
       ~dst_bucket:"my-bucket" ~dst_key:"copy" ~options:copy_options ()
    |> ok_or_fail "copy preconditions");
  match List.rev conn.calls with
  | [ put; get; head; delete; copy ] ->
      Alcotest.(check (option string))
        "put if-match" (Some "\"etag\"")
        (header "if-match" put.request.headers);
      Alcotest.(check (option string))
        "put if-none-match" (Some "*")
        (header "if-none-match" put.request.headers);
      List.iter
        (fun (call : Recording_runtime.call) ->
          Alcotest.(check (option string))
            "read if-match" (Some "\"etag\"")
            (header "if-match" call.request.headers);
          Alcotest.(check (option string))
            "read if-none-match" (Some "*")
            (header "if-none-match" call.request.headers);
          Alcotest.(check (option string))
            "read if-modified-since" (Some time)
            (header "if-modified-since" call.request.headers);
          Alcotest.(check (option string))
            "read if-unmodified-since" (Some time)
            (header "if-unmodified-since" call.request.headers))
        [ get; head ];
      Alcotest.(check (option string))
        "delete if-match" (Some "\"etag\"")
        (header "if-match" delete.request.headers);
      Alcotest.(check (option string))
        "delete last modified" (Some time)
        (header "x-amz-if-match-last-modified-time" delete.request.headers);
      Alcotest.(check (option string))
        "delete size" (Some "4")
        (header "x-amz-if-match-size" delete.request.headers);
      Alcotest.(check (option string))
        "copy source if-match" (Some "\"etag\"")
        (header "x-amz-copy-source-if-match" copy.request.headers);
      Alcotest.(check (option string))
        "copy source if-none-match" (Some "*")
        (header "x-amz-copy-source-if-none-match" copy.request.headers);
      Alcotest.(check (option string))
        "copy source modified since" (Some time)
        (header "x-amz-copy-source-if-modified-since" copy.request.headers);
      Alcotest.(check (option string))
        "copy source unmodified since" (Some time)
        (header "x-amz-copy-source-if-unmodified-since" copy.request.headers)
  | _ -> Alcotest.fail "expected five recorded calls"

let test_object_versioning_requests_and_parse () =
  let version_id = Object.Version_id.of_string_exn "version-1" in
  let next_version_id = Object.Version_id.of_string_exn "version-2" in
  let versions_body =
    {|<ListVersionsResult><Name>my-bucket</Name><Prefix>logs/</Prefix><KeyMarker>logs/a.txt</KeyMarker><VersionIdMarker>version-1</VersionIdMarker><NextKeyMarker>logs/b.txt</NextKeyMarker><NextVersionIdMarker>version-2</NextVersionIdMarker><IsTruncated>true</IsTruncated><Version><Key>logs/a.txt</Key><VersionId>version-1</VersionId><IsLatest>false</IsLatest><LastModified>2026-04-08T12:00:00Z</LastModified><ETag>"etag"</ETag><Size>3</Size><StorageClass>STANDARD</StorageClass></Version><DeleteMarker><Key>logs/a.txt</Key><VersionId>marker-1</VersionId><IsLatest>true</IsLatest><LastModified>2026-04-08T12:00:00Z</LastModified></DeleteMarker></ListVersionsResult>|}
  in
  let conn =
    Recording_runtime.connect
      [
        response 200
          ~headers:
            [
              ("x-amz-version-id", "copy-version");
              ("x-amz-copy-source-version-id", "version-1");
            ]
          {|<CopyObjectResult><ETag>"copy"</ETag></CopyObjectResult>|};
        response 200 versions_body;
      ]
  in
  let copy_options =
    { Object.Copy.default_options with source_version_id = Some version_id }
  in
  let copy =
    Recording_s3.Object.copy conn ~src_bucket:"my-bucket" ~src_key:"file"
      ~dst_bucket:"my-bucket" ~dst_key:"copy" ~options:copy_options ()
    |> ok_or_fail "copy source version"
  in
  Alcotest.(check (option string))
    "copy result source version" (Some "version-1")
    (version_string copy.copy_source_version_id);
  let list_options =
    {
      Object.Versions.default_options with
      prefix = Some "logs/";
      max_keys = Some 10;
      key_marker = Some "logs/a.txt";
      version_id_marker = Some version_id;
    }
  in
  let page =
    Recording_s3.Object.list_versions conn ~bucket:"my-bucket"
      ~options:list_options ()
    |> ok_or_fail "list versions"
  in
  Alcotest.(check bool) "versions truncated" true page.is_truncated;
  Alcotest.(check int) "version count" 1 (List.length page.versions);
  Alcotest.(check int) "delete marker count" 1 (List.length page.delete_markers);
  Alcotest.(check (option string))
    "next key marker" (Some "logs/b.txt") page.next_key_marker;
  Alcotest.(check (option string))
    "next version marker"
    (Some (Object.Version_id.to_string next_version_id))
    (version_string page.next_version_id_marker);
  match List.rev conn.calls with
  | [ copy_call; versions_call ] ->
      Alcotest.(check (option string))
        "copy source header" (Some "/my-bucket/file?versionId=version-1")
        (header "x-amz-copy-source" copy_call.request.headers);
      Alcotest.(check (option (list string)))
        "versions query" (Some [])
        (List.assoc_opt "versions" versions_call.request.target.query);
      Alcotest.(check (option (list string)))
        "prefix query" (Some [ "logs/" ])
        (List.assoc_opt "prefix" versions_call.request.target.query);
      Alcotest.(check (option (list string)))
        "key marker query" (Some [ "logs/a.txt" ])
        (List.assoc_opt "key-marker" versions_call.request.target.query);
      Alcotest.(check (option (list string)))
        "version marker query" (Some [ "version-1" ])
        (List.assoc_opt "version-id-marker" versions_call.request.target.query)
  | _ -> Alcotest.fail "expected copy and version listing calls"

let test_retry_slow_down_then_success () =
  let slow_down =
    {|<Error><Code>SlowDown</Code><Message>reduce request rate</Message></Error>|}
  in
  let conn =
    Recording_runtime.connect [ response 503 slow_down; response 200 "" ]
  in
  let result =
    Recording_s3.Object.Buffer.put_string conn ~bucket:"my-bucket" ~key:"file"
      "body"
  in
  ignore (ok_or_fail "retry put" result);
  Alcotest.(check int) "attempts" 2 (List.length conn.calls);
  Alcotest.(check int) "sleeps" 1 (List.length conn.sleeps)

let test_retry_fatal_error_not_retried () =
  let denied =
    {|<Error><Code>AccessDenied</Code><Message>denied</Message></Error>|}
  in
  let conn =
    Recording_runtime.connect [ response 403 denied; response 200 "" ]
  in
  (match
     Recording_s3.Object.Buffer.put_string conn ~bucket:"my-bucket" ~key:"file"
       "body"
   with
  | Error error when Error.service_code error = Some "AccessDenied" -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected access denied");
  Alcotest.(check int) "attempts" 1 (List.length conn.calls);
  Alcotest.(check int) "sleeps" 0 (List.length conn.sleeps)

let test_retry_disabled_policy () =
  let slow_down =
    {|<Error><Code>SlowDown</Code><Message>reduce request rate</Message></Error>|}
  in
  let conn =
    Recording_runtime.connect ~retry_policy:Awskit.Retry.disabled
      [ response 503 slow_down; response 200 "" ]
  in
  (match
     Recording_s3.Object.Buffer.put_string conn ~bucket:"my-bucket" ~key:"file"
       "body"
   with
  | Error error when Error.service_code error = Some "SlowDown" -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected slow down");
  Alcotest.(check int) "attempts" 1 (List.length conn.calls);
  Alcotest.(check int) "sleeps" 0 (List.length conn.sleeps)

let test_non_replayable_upload_not_retried () =
  let slow_down =
    {|<Error><Code>SlowDown</Code><Message>reduce request rate</Message></Error>|}
  in
  let descriptor : Awskit.Body.Upload.descriptor =
    {
      content_length = Some 4L;
      payload_hash = Awskit.Body.Payload_hash.sha256_of_string "body";
      replayable = false;
    }
  in
  let conn =
    Recording_runtime.connect [ response 503 slow_down; response 200 "" ]
  in
  let body =
    Recording_runtime.stream_body descriptor ~write:(fun writer ->
        Recording_runtime.write_string writer "body")
  in
  (match
     Recording_s3.Object.put conn ~bucket:"my-bucket" ~key:"file" ~body ()
   with
  | Error error when Error.service_code error = Some "SlowDown" -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected slow down");
  Alcotest.(check int) "attempts" 1 (List.length conn.calls);
  Alcotest.(check int) "sleeps" 0 (List.length conn.sleeps)

let test_runtime_stream_upload_error_propagates () =
  let descriptor : Awskit.Body.Upload.descriptor =
    {
      content_length = Some 4L;
      payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
      replayable = false;
    }
  in
  let stream_error = Awskit.Error.body "runtime stream upload failed" in
  let conn = Recording_runtime.connect [ response 200 "" ] in
  let body =
    Recording_runtime.stream_body descriptor ~write:(fun writer ->
        match Recording_runtime.write_string writer "ab" with
        | Error _ as error -> error
        | Ok () -> Error stream_error)
  in
  match
    Recording_s3.Object.put conn ~bucket:"my-bucket" ~key:"file" ~body ()
  with
  | Error error when Error.equal error stream_error -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected stream upload error"

let test_retry_jitter_bounds () =
  let policy =
    Awskit.Retry.create_exn ~max_attempts:3
      ~base_delay:(Ptime.Span.of_float_s 1.0 |> Option.get)
      ~max_delay:(Ptime.Span.of_float_s 10.0 |> Option.get)
      ~jitter:0.5 ()
  in
  let error =
    Awskit.Error.transport ~retryable:true "temporary transport failure"
  in
  let low =
    Awskit.Retry.delay policy ~attempt:1 ~error ~random_float:(fun () -> 0.0)
    |> Option.get
  in
  let high =
    Awskit.Retry.delay policy ~attempt:1 ~error ~random_float:(fun () -> 1.0)
    |> Option.get
  in
  Alcotest.(check (float 0.0001)) "low jitter" 0.5 (Ptime.Span.to_float_s low);
  Alcotest.(check (float 0.0001)) "high jitter" 1.0 (Ptime.Span.to_float_s high);
  match Awskit.Retry.create ~jitter:1.5 () with
  | Error (Awskit.Error.Validation _) -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected invalid jitter"

let test_upload_descriptor_validation () =
  let invalid_descriptor : Awskit.Body.Upload.descriptor =
    {
      content_length = Some (-1L);
      payload_hash = Awskit.Body.Payload_hash.sha256_of_string "";
      replayable = true;
    }
  in
  let conn = Recording_runtime.connect [ response 200 "" ] in
  let body =
    Recording_runtime.stream_body invalid_descriptor ~write:(fun _writer ->
        Ok ())
  in
  (match
     Recording_s3.Object.put conn ~bucket:"my-bucket" ~key:"file" ~body ()
   with
  | Error (Awskit.Error.Validation { field = Some "content_length"; _ }) -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected descriptor validation failure");
  Alcotest.(check int) "object put not called" 0 (List.length conn.calls);
  let upload_id = Multipart.Upload_id.of_string_exn "upload-1" in
  (match
     Recording_s3.Multipart.upload_part conn ~bucket:"my-bucket"
       ~key:"large.bin" ~upload_id ~part_number:1 ~body ()
   with
  | Error (Awskit.Error.Validation { field = Some "content_length"; _ }) -> ()
  | Error error ->
      Alcotest.failf "unexpected multipart error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected multipart descriptor validation failure");
  Alcotest.(check int) "multipart put not called" 0 (List.length conn.calls)

let test_sim_stream_upload_error_propagates () =
  let credentials =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let clock = Sim.Clock.create ~now:test_time () in
  let store = Sim.create_store ~clock () in
  let conn = Sim.connect store ~credentials in
  let bucket = "stream-error-bucket" in
  ignore (Sim.Bucket.create conn ~bucket () |> ok_or_fail "create bucket");
  let stream_error = Awskit.Error.body "sim stream upload failed" in
  let descriptor : Awskit.Body.Upload.descriptor =
    {
      content_length = Some 4L;
      payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
      replayable = false;
    }
  in
  let body =
    Sim.Runtime.stream_body descriptor ~write:(fun writer ->
        match Sim.Runtime.write_string writer "ab" with
        | Error _ as error -> error
        | Ok () -> Error stream_error)
  in
  (match Sim.Object.put conn ~bucket ~key:"bad" ~body () with
  | Error error when Awskit.Error.equal error stream_error -> ()
  | Error error -> Alcotest.failf "unexpected put error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected stream upload error");
  match Sim.Object.head conn ~bucket ~key:"bad" () with
  | Error error when Error.is_not_found error -> ()
  | Error error -> Alcotest.failf "unexpected head error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected stream error object to be absent"

let test_sim_stream_upload_rejects_length_mismatch () =
  let credentials =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let clock = Sim.Clock.create ~now:test_time () in
  let store = Sim.create_store ~clock () in
  let conn = Sim.connect store ~credentials in
  let bucket = "stream-length-bucket" in
  ignore (Sim.Bucket.create conn ~bucket () |> ok_or_fail "create bucket");
  let descriptor : Awskit.Body.Upload.descriptor =
    {
      content_length = Some 4L;
      payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
      replayable = false;
    }
  in
  let short_body =
    Sim.Runtime.stream_body descriptor ~write:(fun writer ->
        Sim.Runtime.write_string writer "ab")
  in
  (match Sim.Object.put conn ~bucket ~key:"short" ~body:short_body () with
  | Error (Awskit.Error.Body _) -> ()
  | Error error ->
      Alcotest.failf "unexpected short put error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected short upload body error");
  let long_body =
    Sim.Runtime.stream_body descriptor ~write:(fun writer ->
        match Sim.Runtime.write_string writer "abcd" with
        | Error _ as error -> error
        | Ok () -> Sim.Runtime.write_string writer "e")
  in
  (match Sim.Object.put conn ~bucket ~key:"long" ~body:long_body () with
  | Error (Awskit.Error.Body _) -> ()
  | Error error -> Alcotest.failf "unexpected long put error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected long upload body error");
  (match Sim.Object.head conn ~bucket ~key:"short" () with
  | Error error when Error.is_not_found error -> ()
  | Error error ->
      Alcotest.failf "unexpected short head error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected short body object to be absent");
  match Sim.Object.head conn ~bucket ~key:"long" () with
  | Error error when Error.is_not_found error -> ()
  | Error error ->
      Alcotest.failf "unexpected long head error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected long body object to be absent"

let test_sim_multipart_upload_part_stream_error_does_not_store_part () =
  let clock = Sim.Clock.create ~now:test_time () in
  let store = Sim.create_store ~clock () in
  let conn = Sim.connect store ~credentials:creds in
  ignore (Sim.Bucket.create conn ~bucket:"test-bucket" () |> ok_or_fail "bucket");
  let created =
    Sim.Multipart.create conn ~bucket:"test-bucket" ~key:"large.bin" ()
    |> ok_or_fail "create multipart upload"
  in
  let upload_id = created.upload.upload_id in
  let stream_error = Awskit.Error.body "sim multipart stream upload failed" in
  let descriptor : Awskit.Body.Upload.descriptor =
    {
      content_length = Some 4L;
      payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
      replayable = false;
    }
  in
  let body =
    Sim.Runtime.stream_body descriptor ~write:(fun writer ->
        match Sim.Runtime.write_string writer "ab" with
        | Error _ as error -> error
        | Ok () -> Error stream_error)
  in
  (match
     Sim.Multipart.upload_part conn ~bucket:"test-bucket" ~key:"large.bin"
       ~upload_id ~part_number:1 ~body ()
   with
  | Error error when Error.equal error stream_error -> ()
  | Error error ->
      Alcotest.failf "unexpected upload part error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected stream upload error");
  let listed =
    Sim.Multipart.list_parts conn ~bucket:"test-bucket" ~key:"large.bin"
      ~upload_id ()
    |> ok_or_fail "list multipart parts"
  in
  Alcotest.(check int) "stored parts" 0 (List.length listed.parts)

let test_download_body_drain_errors () =
  let conn =
    Recording_runtime.connect
      [
        response 200
          ~headers:[ ("etag", "\"etag\""); ("content-length", "6") ]
          ~read_error_after:3 "abcdef";
        response 200
          ~headers:[ ("etag", "\"etag\""); ("content-length", "6") ]
          ~read_error_after:0 "abcdef";
      ]
  in
  let consume reader =
    let bytes = Bytes.create 3 in
    match Recording_runtime.read reader bytes ~off:0 ~len:3 with
    | Error _ as error -> error
    | Ok read -> Ok (Bytes.sub_string bytes 0 read)
  in
  (match
     Recording_s3.Object.get conn ~bucket:"my-bucket" ~key:"file" ~consume ()
   with
  | Error (Awskit.Error.Body _) -> ()
  | Error error -> Alcotest.failf "unexpected get error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected drain failure after successful consume");
  (match Recording_s3.Object.head conn ~bucket:"my-bucket" ~key:"file" () with
  | Error (Awskit.Error.Body _) -> ()
  | Error error -> Alcotest.failf "unexpected head error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected discard failure after successful head");
  Alcotest.(check int) "calls" 2 (List.length conn.calls)

let test_malformed_xml_responses () =
  let conn =
    Recording_runtime.connect
      [
        response 200 "<ListAllMyBucketsResult><Buckets>";
        response 200 "<ListBucketResult><Contents>";
        response 200 "<ListVersionsResult><Version>";
        response 200 "<ListPartsResult><Part>";
      ]
  in
  (match Recording_s3.Bucket.list conn with
  | Error (Awskit.Error.Decode _) -> ()
  | Error error ->
      Alcotest.failf "unexpected bucket decode error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected bucket list decode error");
  (match Recording_s3.Object.list conn ~bucket:"my-bucket" () with
  | Error (Awskit.Error.Decode _) -> ()
  | Error error ->
      Alcotest.failf "unexpected object decode error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected object list decode error");
  (match Recording_s3.Object.list_versions conn ~bucket:"my-bucket" () with
  | Error (Awskit.Error.Decode _) -> ()
  | Error error ->
      Alcotest.failf "unexpected version decode error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected version list decode error");
  let upload_id = Multipart.Upload_id.of_string_exn "upload-1" in
  match
    Recording_s3.Multipart.list_parts conn ~bucket:"my-bucket" ~key:"large.bin"
      ~upload_id ()
  with
  | Error (Awskit.Error.Decode _) -> ()
  | Error error ->
      Alcotest.failf "unexpected multipart decode error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected list parts decode error"

let test_copy_object_embedded_error () =
  let body =
    {|<Error><Code>SlowDown</Code><Message>reduce request rate</Message></Error>|}
  in
  let conn = Recording_runtime.connect [ response 200 body ] in
  match
    Recording_s3.Object.copy conn ~src_bucket:"my-bucket" ~src_key:"file"
      ~dst_bucket:"my-bucket" ~dst_key:"copy" ()
  with
  | Error error when Error.service_code error = Some "SlowDown" -> ()
  | Error error -> Alcotest.failf "unexpected copy error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected embedded copy error"

let list_page ?continuation_token ?next_continuation_token ~truncated keys =
  let token_xml name = function
    | None -> ""
    | Some value -> Fmt.str "<%s>%s</%s>" name value name
  in
  let contents =
    keys
    |> List.map (fun key ->
        Fmt.str
          "<Contents><Key>%s</Key><Size>1</Size><ETag>\"etag\"</ETag></Contents>"
          key)
    |> String.concat ""
  in
  Fmt.str
    "<ListBucketResult><Name>my-bucket</Name><IsTruncated>%b</IsTruncated>%s%s%s</ListBucketResult>"
    truncated
    (token_xml "ContinuationToken" continuation_token)
    (token_xml "NextContinuationToken" next_continuation_token)
    contents

let test_object_paginator_follows_tokens () =
  let conn =
    Recording_runtime.connect
      [
        response 200
          (list_page ~next_continuation_token:"token-1" ~truncated:true
             [ "a.txt" ]);
        response 200
          (list_page ~continuation_token:"token-1" ~truncated:false [ "b.txt" ]);
      ]
  in
  let options = { Object.List.default_options with max_keys = Some 1 } in
  let keys =
    Recording_s3.Object.Paginator.keys conn ~bucket:"my-bucket" ~options ()
    |> ok_or_fail "paginator keys"
  in
  Alcotest.(check (list string)) "keys" [ "a.txt"; "b.txt" ] keys;
  let calls = List.rev conn.calls in
  Alcotest.(check int) "calls" 2 (List.length calls);
  match calls with
  | [ _first; second ] ->
      Alcotest.(check (option (list string)))
        "continuation token" (Some [ "token-1" ])
        (List.assoc_opt "continuation-token" second.request.target.query)
  | _ -> Alcotest.fail "expected two calls"

let test_object_paginator_max_pages () =
  let conn =
    Recording_runtime.connect
      [
        response 200
          (list_page ~next_continuation_token:"token-1" ~truncated:true
             [ "a.txt" ]);
        response 200 (list_page ~truncated:false [ "b.txt" ]);
      ]
  in
  let pages =
    Recording_s3.Object.Paginator.pages conn ~bucket:"my-bucket" ~max_pages:1 ()
    |> ok_or_fail "paginator pages"
  in
  Alcotest.(check int) "page count" 1 (List.length pages);
  Alcotest.(check int) "calls" 1 (List.length conn.calls)

let list_parts_page ?next_part_number_marker ~truncated part_numbers =
  let next_marker_xml =
    match next_part_number_marker with
    | None -> ""
    | Some marker ->
        Fmt.str "<NextPartNumberMarker>%d</NextPartNumberMarker>" marker
  in
  let parts =
    part_numbers
    |> List.map (fun part_number ->
        Fmt.str
          "<Part><PartNumber>%d</PartNumber><ETag>\"etag-%d\"</ETag><Size>1</Size></Part>"
          part_number part_number)
    |> String.concat ""
  in
  Fmt.str "<ListPartsResult><IsTruncated>%b</IsTruncated>%s%s</ListPartsResult>"
    truncated next_marker_xml parts

let test_multipart_paginator_follows_markers () =
  let conn =
    Recording_runtime.connect
      [
        response 200
          (list_parts_page ~next_part_number_marker:1 ~truncated:true [ 1 ]);
        response 200 (list_parts_page ~truncated:false [ 2 ]);
      ]
  in
  let upload_id = Multipart.Upload_id.of_string_exn "upload-1" in
  let parts =
    Recording_s3.Multipart.Paginator.parts conn ~bucket:"my-bucket"
      ~key:"large.bin" ~upload_id ()
    |> ok_or_fail "multipart paginator parts"
  in
  Alcotest.(check (list int))
    "parts" [ 1; 2 ]
    (List.map
       (fun (part : Multipart.List_parts.part_info) -> part.part_number)
       parts);
  let calls = List.rev conn.calls in
  Alcotest.(check int) "calls" 2 (List.length calls);
  match calls with
  | [ _first; second ] ->
      Alcotest.(check (option (list string)))
        "part marker" (Some [ "1" ])
        (List.assoc_opt "part-number-marker" second.request.target.query)
  | _ -> Alcotest.fail "expected two calls"

let test_multipart_upload_part_checksum_headers () =
  let conn =
    Recording_runtime.connect
      [
        response 200
          ~headers:
            [ ("etag", "\"etag-1\""); ("x-amz-checksum-sha1", "provided-sha1") ]
          "";
      ]
  in
  let upload_id = Multipart.Upload_id.of_string_exn "upload-1" in
  let checksum : Object.Checksum.request =
    { Object.Checksum.algorithm = `SHA1; value = Some "provided-sha1" }
  in
  let options = { Multipart.Upload_part.checksum = Some checksum } in
  let part =
    Recording_s3.Multipart.upload_part conn ~bucket:"my-bucket" ~key:"large.bin"
      ~upload_id ~part_number:1
      ~body:(Recording_runtime.string_body "hello")
      ~options ()
    |> ok_or_fail "upload part checksum"
  in
  check_checksum "part response checksum" `SHA1 "provided-sha1" part.checksum;
  let call = Recording_runtime.last_call conn in
  Alcotest.(check (option string))
    "checksum algorithm header" (Some "SHA1")
    (header "x-amz-checksum-algorithm" call.request.headers);
  Alcotest.(check (option string))
    "checksum value header" (Some "provided-sha1")
    (header "x-amz-checksum-sha1" call.request.headers);
  Alcotest.(check (option (list string)))
    "part number" (Some [ "1" ])
    (List.assoc_opt "partNumber" call.request.target.query)

let multipart_create_body upload_id =
  Fmt.str
    "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>"
    upload_id

let multipart_complete_body etag =
  Fmt.str
    "<CompleteMultipartUploadResult><ETag>%s</ETag></CompleteMultipartUploadResult>"
    etag

let test_managed_multipart_upload_string () =
  let part_size = Multipart.Managed.min_part_size in
  let body = String.make part_size 'a' ^ "end" in
  let conn =
    Recording_runtime.connect
      [
        response 200 (multipart_create_body "upload-1");
        response 200 ~headers:[ ("etag", "\"part-1\"") ] "";
        response 200 ~headers:[ ("etag", "\"part-2\"") ] "";
        response 200 (multipart_complete_body "\"complete\"");
      ]
  in
  let options = { Multipart.Managed.default_options with part_size } in
  let result =
    Recording_s3.Multipart.Managed.upload_string conn ~bucket:"my-bucket"
      ~key:"large.bin" ~options body
    |> ok_or_fail "managed multipart upload"
  in
  Alcotest.(check (list int))
    "uploaded parts" [ 1; 2 ]
    (List.map (fun (part : Multipart.Part.t) -> part.part_number) result.parts);
  Alcotest.(check bool)
    "complete etag" true
    (Option.is_some result.complete.etag);
  match List.rev conn.calls with
  | [ create; part1; part2; complete ] ->
      Alcotest.(check string)
        "create method" "POST"
        (Awskit.Request.Method.to_string create.request.method_);
      Alcotest.(check (option (list string)))
        "create uploads query" (Some [])
        (List.assoc_opt "uploads" create.request.target.query);
      Alcotest.(check int) "part 1 body" part_size (String.length part1.body);
      Alcotest.(check string) "part 2 body" "end" part2.body;
      Alcotest.(check (option (list string)))
        "part 1 number" (Some [ "1" ])
        (List.assoc_opt "partNumber" part1.request.target.query);
      Alcotest.(check (option (list string)))
        "part 2 number" (Some [ "2" ])
        (List.assoc_opt "partNumber" part2.request.target.query);
      Alcotest.(check bool)
        "complete includes first part" true
        (String.contains complete.body '1');
      Alcotest.(check bool)
        "complete includes second etag" true
        (String.contains complete.body '2')
  | _ -> Alcotest.fail "expected create, two parts, complete"

let test_complete_multipart_embedded_error () =
  let body =
    {|<Error><Code>SlowDown</Code><Message>reduce request rate</Message></Error>|}
  in
  let conn = Recording_runtime.connect [ response 200 body ] in
  let upload_id = Multipart.Upload_id.of_string_exn "upload-1" in
  let part =
    Multipart.Part.create_exn ~part_number:1
      ~etag:(Object.Etag.of_string_exn "\"part-1\"")
  in
  match
    Recording_s3.Multipart.complete conn ~bucket:"my-bucket" ~key:"large.bin"
      ~upload_id [ part ]
  with
  | Error error when Error.service_code error = Some "SlowDown" -> ()
  | Error error -> Alcotest.failf "unexpected complete error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected embedded complete error"

let test_managed_multipart_aborts_on_part_failure () =
  let part_size = Multipart.Managed.min_part_size in
  let body = String.make part_size 'x' in
  let conn =
    Recording_runtime.connect
      [
        response 200 (multipart_create_body "upload-1");
        response 400
          {|<Error><Code>InvalidRequest</Code><Message>bad part</Message></Error>|};
        response 204 "";
      ]
  in
  let options = { Multipart.Managed.default_options with part_size } in
  (match
     Recording_s3.Multipart.Managed.upload_string conn ~bucket:"my-bucket"
       ~key:"large.bin" ~options body
   with
  | Error error when Error.service_code error = Some "InvalidRequest" -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected managed upload failure");
  match List.rev conn.calls with
  | [ _create; _part1; abort ] ->
      Alcotest.(check string)
        "abort method" "DELETE"
        (Awskit.Request.Method.to_string abort.request.method_);
      Alcotest.(check (option (list string)))
        "abort upload id" (Some [ "upload-1" ])
        (List.assoc_opt "uploadId" abort.request.target.query)
  | _ -> Alcotest.fail "expected create, failed part, abort"

let make_sim () =
  let clock = Sim.Clock.create ~now:test_time () in
  let store = Sim.create_store ~clock () in
  let conn = Sim.connect store ~credentials:creds in
  ignore (Sim.Bucket.create conn ~bucket:"test-bucket" () |> ok_or_fail "bucket");
  conn

let test_sim_buffer_roundtrip () =
  let conn = make_sim () in
  let checksum : Object.Checksum.request =
    { Object.Checksum.algorithm = `SHA256; value = None }
  in
  let put =
    Sim.Object.Buffer.put_string conn ~bucket:"test-bucket" ~key:"hello.txt"
      ~options:
        {
          Object.Put.default_options with
          content_type = Some "text/plain";
          checksum = Some checksum;
        }
      "hello"
    |> ok_or_fail "put"
  in
  Alcotest.(check bool) "etag" true (Option.is_some put.etag);
  check_checksum "put checksum" `SHA256
    "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" put.checksum;
  let info, body =
    Sim.Object.Buffer.get_string conn ~bucket:"test-bucket" ~key:"hello.txt"
      ~max_size:16L ()
    |> ok_or_fail "get"
  in
  Alcotest.(check string) "body" "hello" body;
  Alcotest.(check (option string))
    "content-type" (Some "text/plain") info.content_type;
  check_checksum "get checksum" `SHA256
    "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" info.checksum;
  let head =
    Sim.Object.head conn ~bucket:"test-bucket" ~key:"hello.txt" ()
    |> ok_or_fail "head checksum"
  in
  check_checksum "head checksum" `SHA256
    "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=" head.checksum;
  let page =
    Sim.Object.list conn ~bucket:"test-bucket" () |> ok_or_fail "list checksum"
  in
  match page.objects with
  | [ object_ ] ->
      Alcotest.(check (option string))
        "listed checksum" (Some "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=")
        (List.find_map
           (fun (checksum : Object.Checksum.response) -> Some checksum.value)
           object_.checksums)
  | _ -> Alcotest.fail "expected one listed object"

let test_sim_streaming_get () =
  let conn = make_sim () in
  ignore
    (Sim.Object.Buffer.put_string conn ~bucket:"test-bucket" ~key:"stream"
       "abcdef"
    |> ok_or_fail "put");
  let consume reader =
    let bytes = Bytes.create 3 in
    match Sim.Runtime.read reader bytes ~off:0 ~len:3 with
    | Error _ as error -> error
    | Ok read -> Ok (Bytes.sub_string bytes 0 read)
  in
  let _info, body =
    Sim.Object.get conn ~bucket:"test-bucket" ~key:"stream" ~consume ()
    |> ok_or_fail "stream get"
  in
  Alcotest.(check string) "partial body" "abc" body

let test_buffer_limit () =
  let conn = make_sim () in
  ignore
    (Sim.Object.Buffer.put_string conn ~bucket:"test-bucket" ~key:"large"
       "abcdef"
    |> ok_or_fail "put");
  match
    Sim.Object.Buffer.get_string conn ~bucket:"test-bucket" ~key:"large"
      ~max_size:3L ()
  with
  | Error (Awskit.Error.Body _) -> ()
  | Error error -> Alcotest.failf "unexpected error: %a" Error.pp error
  | Ok _ -> Alcotest.fail "expected max_size failure"

let test_sim_paginator_keys () =
  let conn = make_sim () in
  ignore
    (Sim.Object.Buffer.put_string conn ~bucket:"test-bucket" ~key:"logs/a.txt"
       "a"
    |> ok_or_fail "put a");
  ignore
    (Sim.Object.Buffer.put_string conn ~bucket:"test-bucket" ~key:"logs/b.txt"
       "b"
    |> ok_or_fail "put b");
  ignore
    (Sim.Object.Buffer.put_string conn ~bucket:"test-bucket" ~key:"other.txt"
       "other"
    |> ok_or_fail "put other");
  let options =
    {
      Object.List.default_options with
      prefix = Some "logs/";
      max_keys = Some 1;
    }
  in
  let keys =
    Sim.Object.Paginator.keys conn ~bucket:"test-bucket" ~options ()
    |> ok_or_fail "sim paginator keys"
  in
  Alcotest.(check (list string)) "keys" [ "logs/a.txt"; "logs/b.txt" ] keys

let suite =
  [
    ( "core",
      [
        Alcotest.test_case "presigned result" `Quick test_presigned_result;
        Alcotest.test_case "presigned put checksum headers" `Quick
          test_presigned_put_checksum_headers;
        Alcotest.test_case "presigned multipart upload part" `Quick
          test_presigned_upload_part;
        Alcotest.test_case "presigned rejects header newline" `Quick
          test_presigned_rejects_header_newline;
        Alcotest.test_case "endpoint resolution" `Quick test_endpoint_resolution;
        Alcotest.test_case "endpoint variants" `Quick test_endpoint_variants;
        Alcotest.test_case "error classifiers" `Quick test_error_classifiers;
        Alcotest.test_case "bucket head request" `Quick test_bucket_head_request;
        Alcotest.test_case "bucket list parse" `Quick test_bucket_list_parse;
        Alcotest.test_case "bucket config parse" `Quick test_bucket_config_parse;
        Alcotest.test_case "object checksum headers and response" `Quick
          test_object_checksum_headers_and_response;
        Alcotest.test_case "object precondition headers" `Quick
          test_object_precondition_headers;
        Alcotest.test_case "object versioning requests and parse" `Quick
          test_object_versioning_requests_and_parse;
        Alcotest.test_case "retry slow down then success" `Quick
          test_retry_slow_down_then_success;
        Alcotest.test_case "retry fatal error not retried" `Quick
          test_retry_fatal_error_not_retried;
        Alcotest.test_case "retry disabled policy" `Quick
          test_retry_disabled_policy;
        Alcotest.test_case "non-replayable upload not retried" `Quick
          test_non_replayable_upload_not_retried;
        Alcotest.test_case "runtime stream upload error propagates" `Quick
          test_runtime_stream_upload_error_propagates;
        Alcotest.test_case "retry jitter bounds" `Quick test_retry_jitter_bounds;
        Alcotest.test_case "upload descriptor validation" `Quick
          test_upload_descriptor_validation;
        Alcotest.test_case "sim stream upload error propagates" `Quick
          test_sim_stream_upload_error_propagates;
        Alcotest.test_case "sim stream upload rejects length mismatch" `Quick
          test_sim_stream_upload_rejects_length_mismatch;
        Alcotest.test_case
          "sim multipart upload part stream error does not store part" `Quick
          test_sim_multipart_upload_part_stream_error_does_not_store_part;
        Alcotest.test_case "download body drain errors" `Quick
          test_download_body_drain_errors;
        Alcotest.test_case "malformed xml responses" `Quick
          test_malformed_xml_responses;
        Alcotest.test_case "copy object embedded error" `Quick
          test_copy_object_embedded_error;
        Alcotest.test_case "object paginator follows tokens" `Quick
          test_object_paginator_follows_tokens;
        Alcotest.test_case "object paginator max pages" `Quick
          test_object_paginator_max_pages;
        Alcotest.test_case "multipart paginator follows markers" `Quick
          test_multipart_paginator_follows_markers;
        Alcotest.test_case "multipart upload part checksum headers" `Quick
          test_multipart_upload_part_checksum_headers;
        Alcotest.test_case "managed multipart upload string" `Quick
          test_managed_multipart_upload_string;
        Alcotest.test_case "complete multipart embedded error" `Quick
          test_complete_multipart_embedded_error;
        Alcotest.test_case "managed multipart aborts on part failure" `Quick
          test_managed_multipart_aborts_on_part_failure;
        Alcotest.test_case "sim buffer roundtrip" `Quick
          test_sim_buffer_roundtrip;
        Alcotest.test_case "sim streaming get" `Quick test_sim_streaming_get;
        Alcotest.test_case "buffer limit" `Quick test_buffer_limit;
        Alcotest.test_case "sim paginator keys" `Quick test_sim_paginator_keys;
      ] );
  ]
