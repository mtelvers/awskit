open Core

module Make (R : RUNTIME) = struct
  type connection = R.connection
  type 'a io = 'a R.t
  type upload_body = R.upload_body
  type download_reader = R.download_reader

  module Context = struct
    module R = R

    type connection = R.connection
    type 'a io = 'a R.t
    type upload_body = R.upload_body
    type download_reader = R.download_reader

    let bind = R.bind
    let ( let* ) = R.bind
    let return = R.return
    let return_ok value = R.return (Ok value)
    let return_error error = R.return (Error error)
    let empty_hash = Awskit.Body.Payload_hash.sha256_of_string ""
    let endpoint_config conn = R.s3_endpoint_config conn

    let object_request conn ~bucket ~key =
      Endpoint_resolver.resolve_object_request (endpoint_config conn)
        ~region:(R.region conn) ~bucket ~key

    let bucket_request conn ~bucket ~suffix ~signing_suffix =
      Endpoint_resolver.resolve_bucket_request (endpoint_config conn)
        ~region:(R.region conn) ~bucket ~suffix ~signing_suffix

    let root_request conn =
      match
        Endpoint_resolver.endpoint (endpoint_config conn)
          ~region:(R.region conn)
      with
      | Error _ as error -> error
      | Ok endpoint ->
          Ok
            {
              Endpoint_resolver.Request.endpoint;
              path = "/";
              signing_path = "/";
              style = `Path;
            }

    let read_body reader ~max_size =
      let buffer = Buffer.create 4096 in
      let chunk = Bytes.create 8192 in
      let rec loop total =
        let* read = R.read reader chunk ~off:0 ~len:(Bytes.length chunk) in
        match read with
        | Error error -> return (Error error)
        | Ok 0 -> return_ok (Buffer.contents buffer)
        | Ok n ->
            let total = Int64.add total (Int64.of_int n) in
            if Int64.compare total max_size > 0 then
              return_error
                (Awskit.Error.body ~limit:max_size
                   "download body exceeded max_size")
            else begin
              Buffer.add_subbytes buffer chunk 0 n;
              loop total
            end
      in
      loop 0L

    let read_download_body body ~max_size =
      R.with_download_body body ~consume:(read_body ~max_size)

    let discard_download_body = R.discard_download_body

    let service_error response body =
      Awskit.Error.service
        {
          status = Awskit.Response.status response;
          code = Option.bind body Xml.service_code;
          message = Option.bind body Xml.service_message;
          request_id = Awskit.Response.request_id response;
          host_id = Awskit.Response.host_id response;
          headers = Awskit.Response.headers response;
          body;
        }

    let error_response response body =
      let* body = read_download_body body ~max_size:1_048_576L in
      match body with
      | Error error -> return_error error
      | Ok body -> return_error (service_error response (Some body))

    let signed_request conn ~method_ ~(request : Endpoint_resolver.Request.t)
        ~query ~headers ~payload_hash =
      let headers =
        ("host", Awskit.Endpoint.authority request.endpoint) :: headers
      in
      let* credentials = R.credentials conn in
      match credentials with
      | Error error -> return_error error
      | Ok credentials -> (
          match
            Awskit.Signing.sign_request_params ~credentials
              ~region:(R.region conn) ~service:"s3" ~method_
              ~path:request.signing_path ~query_params:query ~headers
              ~payload_hash ~now:(R.now conn)
          with
          | Error error -> return_error error
          | Ok signed -> (
              match
                Awskit.Request.Target.create
                  ~scheme:(Awskit.Endpoint.scheme request.endpoint)
                  ~host:(Awskit.Endpoint.host request.endpoint)
                  ?port:(Awskit.Endpoint.port request.endpoint)
                  ~path:request.path ~query ()
              with
              | Error error -> return_error error
              | Ok target -> (
                  match
                    Awskit.Request.create ~method_ ~target
                      ~headers:signed.headers ()
                  with
                  | Error error -> return_error error
                  | Ok request -> return_ok request)))

    let retry_or_error conn ~attempt ~replayable error retry =
      match Awskit.Retry.delay (R.retry_policy conn) ~attempt ~error with
      | Some delay when replayable ->
          let* () = R.sleep conn delay in
          retry (attempt + 1)
      | _ -> return_error error

    type 'a response_action = Done of ('a, Error.t) result | Retry of Error.t

    let with_response conn ~method_ ~request ~query ~headers ~payload_hash body
        ~f =
      let replayable = (R.upload_descriptor body).replayable in
      let rec attempt attempt_number =
        let* request =
          signed_request conn ~method_ ~request ~query ~headers ~payload_hash
        in
        match request with
        | Error error -> return_error error
        | Ok request -> (
            let* response =
              R.with_response conn request body
                ~f:(fun response response_body ->
                  if Awskit.Response.is_success response then
                    let* result = f response response_body in
                    return_ok (Done result)
                  else
                    let* body =
                      read_download_body response_body ~max_size:1_048_576L
                    in
                    match body with
                    | Error error -> return_ok (Done (Error error))
                    | Ok body ->
                        return_ok (Retry (service_error response (Some body))))
            in
            match response with
            | Error error ->
                retry_or_error conn ~attempt:attempt_number ~replayable error
                  attempt
            | Ok (Done result) -> return result
            | Ok (Retry error) ->
                retry_or_error conn ~attempt:attempt_number ~replayable error
                  attempt)
      in
      attempt 1

    let with_empty_response conn ~method_ ~request ~query ~headers ~f =
      with_response conn ~method_ ~request ~query ~headers
        ~payload_hash:empty_hash R.empty_body ~f

    let content_md5 body =
      Digestif.MD5.(digest_string body |> to_raw_string) |> Base64.encode_exn
  end

  module Object = Object_ops.Make (Context)
  module Bucket = Bucket_ops.Make (Context)
  module Multipart = Multipart_ops.Make (Context)
  module Presigned = Presigned_ops.Make (Context)
end
