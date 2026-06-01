type t = {
  aws : Awskit_lwt_unix.t;
  endpoint_config : Awskit_s3.endpoint_config;
}

module Runtime = struct
  type connection = t
  type 'a t = 'a Lwt.t
  type upload_body = Awskit_lwt_unix.Runtime.upload_body
  type download_body = Awskit_lwt_unix.Runtime.download_body
  type upload_writer = Awskit_lwt_unix.Runtime.upload_writer
  type download_reader = Awskit_lwt_unix.Runtime.download_reader

  let return = Awskit_lwt_unix.Runtime.return
  let bind = Awskit_lwt_unix.Runtime.bind
  let now t = Awskit_lwt_unix.Runtime.now t.aws
  let region t = Awskit_lwt_unix.Runtime.region t.aws
  let credentials t = Awskit_lwt_unix.Runtime.credentials t.aws
  let retry_policy t = Awskit_lwt_unix.Runtime.retry_policy t.aws
  let sleep t = Awskit_lwt_unix.Runtime.sleep t.aws
  let s3_endpoint_config t = t.endpoint_config
  let endpoint _ = None
  let empty_body = Awskit_lwt_unix.Runtime.empty_body
  let string_body = Awskit_lwt_unix.Runtime.string_body
  let bytes_body = Awskit_lwt_unix.Runtime.bytes_body
  let stream_body = Awskit_lwt_unix.Runtime.stream_body
  let upload_descriptor = Awskit_lwt_unix.Runtime.upload_descriptor
  let write_string = Awskit_lwt_unix.Runtime.write_string
  let read = Awskit_lwt_unix.Runtime.read
  let with_download_body = Awskit_lwt_unix.Runtime.with_download_body
  let discard_download_body = Awskit_lwt_unix.Runtime.discard_download_body

  let with_response t request body ~f =
    Awskit_lwt_unix.Runtime.with_response t.aws request body ~f
end

module S3 = Awskit_s3.Make (Runtime)

let create ?ctx ?endpoint ?addressing_style ?endpoint_variant ?scheme ?region
    ?credentials ?clock ?retry_policy ?imdsv1_fallback () =
  let region = Option.map Awskit.Region.to_string region in
  match
    Awskit_lwt_unix.create ?ctx ?region ?credentials ?clock ?retry_policy
      ?imdsv1_fallback ()
  with
  | Error _ as error -> error
  | Ok aws ->
      let endpoint_config =
        Awskit_s3.endpoint_config ?addressing_style ?endpoint_variant ?scheme
          ?endpoint ()
      in
      Ok { aws; endpoint_config }

module Object = struct
  include S3.Object
  module Transfer = Transfer.Make (Runtime) (S3)
end

module Bucket = S3.Bucket
module Multipart = S3.Multipart
module Presigned = S3.Presigned
