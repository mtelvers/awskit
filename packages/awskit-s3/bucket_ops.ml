open Core

module Make (C : Operation_context.S) = struct
  open C

  let ( let* ) = bind

  type nonrec connection = connection
  type 'a io = 'a R.t

  module Bucket_xml = Bucket_config_xml

  let bucket_root_request conn bucket =
    bucket_request conn ~bucket ~suffix:"/" ~signing_suffix:"/"

  let content_md5_header body = ("content-md5", content_md5 body)

  let get_xml conn ~bucket ~subresource ~max_size ~parse =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`GET ~request
              ~query:[ (subresource, []) ]
              ~headers:[]
              ~f:(fun response body ->
                let* body = read_download_body body ~max_size in
                match body with
                | Error error -> return_error error
                | Ok body -> return (parse body response)))

  let put_xml conn ~bucket ~subresource ~body =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        let upload = R.string_body body in
        let headers =
          [ content_md5_header body; ("content-type", "application/xml") ]
        in
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_response conn ~method_:`PUT ~request
              ~query:[ (subresource, []) ]
              ~headers ~payload_hash:(R.upload_descriptor upload).payload_hash
              upload
              ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return_ok response))

  let delete_subresource conn ~bucket ~subresource =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`DELETE ~request
              ~query:[ (subresource, []) ]
              ~headers:[]
              ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return_ok response))

  let create conn ~bucket ?options () =
    let options = Option.value ~default:Bucket.Create.default_options options in
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        let region = Option.value ~default:(R.region conn) options.region in
        let body =
          if
            Awskit.Region.equal region (Awskit.Region.of_string_exn "us-east-1")
          then ""
          else Bucket_result_xml.create_config region
        in
        let upload = R.string_body body in
        let headers =
          if body = "" then [] else [ ("content-type", "application/xml") ]
        in
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_response conn ~method_:`PUT ~request ~query:[] ~headers
              ~payload_hash:(R.upload_descriptor upload).payload_hash upload
              ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return_ok { Bucket.Create.request = response }))

  let delete conn ~bucket =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`DELETE ~request ~query:[]
              ~headers:[] ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return_ok { Bucket.Delete.request = response }))

  let head conn ~bucket =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`HEAD ~request ~query:[]
              ~headers:[] ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () ->
                    let region =
                      Option.bind
                        (Awskit.Response.header response "x-amz-bucket-region")
                        (fun value ->
                          Result.to_option (Awskit.Region.of_string value))
                    in
                    return_ok
                      { Bucket.Head.name = bucket; region; request = response })
        )

  let exists conn ~bucket =
    let* result = head conn ~bucket in
    match result with
    | Ok _ -> return_ok true
    | Error error when Error.is_not_found error -> return_ok false
    | Error error -> return_error error

  let list conn =
    match root_request conn with
    | Error error -> return_error error
    | Ok request ->
        with_empty_response conn ~method_:`GET ~request ~query:[] ~headers:[]
          ~f:(fun _response body ->
            let* body = read_download_body body ~max_size:4_194_304L in
            match body with
            | Error error -> return_error error
            | Ok body -> return (Bucket_result_xml.parse_list body))

  let get_location conn ~bucket =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        match bucket_root_request conn bucket with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`GET ~request
              ~query:[ ("location", []) ]
              ~headers:[]
              ~f:(fun _response body ->
                let* body = read_download_body body ~max_size:1_048_576L in
                match body with
                | Error error -> return_error error
                | Ok body -> return (Bucket_result_xml.parse_location body)))

  module Policy = struct
    let get conn ~bucket =
      match validate_bucket bucket with
      | Error error -> return_error error
      | Ok () -> (
          match bucket_root_request conn bucket with
          | Error error -> return_error error
          | Ok request ->
              with_empty_response conn ~method_:`GET ~request
                ~query:[ ("policy", []) ]
                ~headers:[]
                ~f:(fun _response body ->
                  let* body = read_download_body body ~max_size:1_048_576L in
                  match body with
                  | Error error -> return_error error
                  | Ok body -> return (Policy.of_json body)))

    let put conn ~bucket policy =
      match validate_bucket bucket with
      | Error error -> return_error error
      | Ok () -> (
          let body = Policy.to_json policy in
          let upload = R.string_body body in
          let headers =
            [ content_md5_header body; ("content-type", "application/json") ]
          in
          match bucket_root_request conn bucket with
          | Error error -> return_error error
          | Ok request ->
              with_response conn ~method_:`PUT ~request
                ~query:[ ("policy", []) ]
                ~headers ~payload_hash:(R.upload_descriptor upload).payload_hash
                upload
                ~f:(fun response body ->
                  let* discarded = discard_download_body body in
                  match discarded with
                  | Error error -> return_error error
                  | Ok () -> return_ok response))

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"policy"
  end

  module Policy_status = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"policyStatus" ~max_size:1_048_576L
        ~parse:Bucket_xml.Policy_status.parse
  end

  module Versioning = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"versioning" ~max_size:1_048_576L
        ~parse:Bucket_xml.Versioning.parse

    let put conn ~bucket status =
      put_xml conn ~bucket ~subresource:"versioning"
        ~body:(Bucket_xml.Versioning.xml (Some status))
  end

  module Tagging = struct
    let parse body response =
      Result.map
        (fun tags -> { Bucket.Tagging.tags; request = response })
        (parse_tags body)

    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"tagging" ~max_size:1_048_576L ~parse

    let put conn ~bucket tags =
      match validate_tags tags with
      | Error error -> return_error error
      | Ok () ->
          put_xml conn ~bucket ~subresource:"tagging" ~body:(xml_tags tags)

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"tagging"
  end

  module Encryption = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"encryption" ~max_size:1_048_576L
        ~parse:Bucket_xml.Encryption.parse

    let put conn ~bucket config =
      match Bucket_xml.Encryption.validate_config config with
      | Error error -> return_error error
      | Ok () ->
          put_xml conn ~bucket ~subresource:"encryption"
            ~body:(Bucket_xml.Encryption.xml config)

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"encryption"
  end

  module Cors = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"cors" ~max_size:1_048_576L
        ~parse:Bucket_xml.Cors.parse

    let put conn ~bucket config =
      match Bucket_xml.Cors.validate_config config with
      | Error error -> return_error error
      | Ok () ->
          put_xml conn ~bucket ~subresource:"cors"
            ~body:(Bucket_xml.Cors.xml config)

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"cors"
  end

  module Website = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"website" ~max_size:1_048_576L
        ~parse:Bucket_xml.Website.parse

    let put conn ~bucket config =
      match Bucket_xml.Website.validate_config config with
      | Error error -> return_error error
      | Ok () ->
          put_xml conn ~bucket ~subresource:"website"
            ~body:(Bucket_xml.Website.xml config)

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"website"
  end

  module Public_access_block = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"publicAccessBlock" ~max_size:1_048_576L
        ~parse:Bucket_xml.Public_access_block.parse

    let put conn ~bucket config =
      put_xml conn ~bucket ~subresource:"publicAccessBlock"
        ~body:(Bucket_xml.Public_access_block.xml config)

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"publicAccessBlock"
  end

  module Ownership_controls = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"ownershipControls" ~max_size:1_048_576L
        ~parse:Bucket_xml.Ownership_controls.parse

    let put conn ~bucket config =
      put_xml conn ~bucket ~subresource:"ownershipControls"
        ~body:(Bucket_xml.Ownership_controls.xml config)

    let delete conn ~bucket =
      delete_subresource conn ~bucket ~subresource:"ownershipControls"
  end

  module Request_payment = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"requestPayment" ~max_size:1_048_576L
        ~parse:Bucket_xml.Request_payment.parse

    let put conn ~bucket payer =
      put_xml conn ~bucket ~subresource:"requestPayment"
        ~body:(Bucket_xml.Request_payment.xml payer)
  end

  module Accelerate = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"accelerate" ~max_size:1_048_576L
        ~parse:Bucket_xml.Accelerate.parse

    let put conn ~bucket status =
      put_xml conn ~bucket ~subresource:"accelerate"
        ~body:(Bucket_xml.Accelerate.xml status)
  end

  module Logging = struct
    let get conn ~bucket =
      get_xml conn ~bucket ~subresource:"logging" ~max_size:1_048_576L
        ~parse:Bucket_xml.Logging.parse

    let put conn ~bucket config =
      match Bucket_xml.Logging.validate_config config with
      | Error error -> return_error error
      | Ok () ->
          put_xml conn ~bucket ~subresource:"logging"
            ~body:(Bucket_xml.Logging.xml config)
  end
end
