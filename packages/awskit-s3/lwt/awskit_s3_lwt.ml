module Make (Client : Cohttp_lwt.S.Client) = struct
  module Aws = Awskit_lwt.Make (Client)

  type t = { aws : Aws.t; endpoint_config : Awskit_s3.endpoint_config }

  module Runtime = struct
    type connection = t
    type 'a t = 'a Lwt.t
    type upload_body = Aws.Runtime.upload_body
    type download_body = Aws.Runtime.download_body
    type upload_writer = Aws.Runtime.upload_writer
    type download_reader = Aws.Runtime.download_reader

    let return = Aws.Runtime.return
    let bind = Aws.Runtime.bind
    let now t = Aws.Runtime.now t.aws
    let region t = Aws.Runtime.region t.aws
    let credentials t = Aws.Runtime.credentials t.aws
    let retry_policy t = Aws.Runtime.retry_policy t.aws
    let sleep t = Aws.Runtime.sleep t.aws
    let s3_endpoint_config t = t.endpoint_config
    let endpoint _ = None
    let empty_body = Aws.Runtime.empty_body
    let string_body = Aws.Runtime.string_body
    let bytes_body = Aws.Runtime.bytes_body
    let stream_body = Aws.Runtime.stream_body
    let upload_descriptor = Aws.Runtime.upload_descriptor
    let write_string = Aws.Runtime.write_string
    let read = Aws.Runtime.read
    let with_download_body = Aws.Runtime.with_download_body
    let discard_download_body = Aws.Runtime.discard_download_body

    let with_response t request body ~f =
      Aws.Runtime.with_response t.aws request body ~f
  end

  module S3 = Awskit_s3.Make (Runtime)

  let create ?ctx ?endpoint ?addressing_style ?endpoint_variant ?scheme ~region
      ~credentials ~clock ?retry_policy ?sleep () =
    let aws =
      Aws.create ?ctx ~region ~credentials ~clock ?retry_policy ?sleep ()
    in
    let endpoint_config =
      Awskit_s3.endpoint_config ?addressing_style ?endpoint_variant ?scheme
        ?endpoint ()
    in
    { aws; endpoint_config }

  module Object = S3.Object
  module Bucket = S3.Bucket
  module Multipart = S3.Multipart
  module Presigned = S3.Presigned
end
