type t = { aws : Awskit_eio.t; endpoint_config : Awskit_s3.endpoint_config }

module Runtime = struct
  type connection = t
  type 'a t = 'a
  type upload_body = Awskit_eio.Runtime.upload_body
  type download_body = Awskit_eio.Runtime.download_body
  type upload_writer = Awskit_eio.Runtime.upload_writer
  type download_reader = Awskit_eio.Runtime.download_reader

  let return = Awskit_eio.Runtime.return
  let bind = Awskit_eio.Runtime.bind
  let now t = Awskit_eio.Runtime.now t.aws
  let region t = Awskit_eio.Runtime.region t.aws
  let credentials t = Awskit_eio.Runtime.credentials t.aws
  let retry_policy t = Awskit_eio.Runtime.retry_policy t.aws
  let sleep t = Awskit_eio.Runtime.sleep t.aws
  let s3_endpoint_config t = t.endpoint_config
  let endpoint _ = None
  let empty_body = Awskit_eio.Runtime.empty_body
  let string_body = Awskit_eio.Runtime.string_body
  let bytes_body = Awskit_eio.Runtime.bytes_body
  let stream_body = Awskit_eio.Runtime.stream_body
  let upload_descriptor = Awskit_eio.Runtime.upload_descriptor
  let write_string = Awskit_eio.Runtime.write_string
  let read = Awskit_eio.Runtime.read
  let with_download_body = Awskit_eio.Runtime.with_download_body
  let discard_download_body = Awskit_eio.Runtime.discard_download_body

  let with_response t request body ~f =
    Awskit_eio.Runtime.with_response t.aws request body ~f
end

module S3 = Awskit_s3.Make (Runtime)

let create ~sw ~env ~region ~credentials ?retry_policy ?endpoint
    ?addressing_style ?endpoint_variant ?scheme () =
  let aws = Awskit_eio.create ~sw ~env ~region ~credentials ?retry_policy () in
  let endpoint_config =
    Awskit_s3.endpoint_config ?addressing_style ?endpoint_variant ?scheme
      ?endpoint ()
  in
  { aws; endpoint_config }

module Object = struct
  include S3.Object
  module Transfer = Transfer.Make (Runtime) (S3)
end

module Bucket = S3.Bucket
module Multipart = S3.Multipart
module Presigned = S3.Presigned
