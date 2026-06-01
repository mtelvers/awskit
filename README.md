# Awskit

Awskit is AWS infrastructure for OCaml.

The core packages are pure: they define credentials, regions, endpoints,
SigV4 signing, request and response types, retry policy, error values, S3 wire
types, request builders, and deterministic simulation. Runtime packages sit at
the edge and provide concrete HTTP execution for Eio or Lwt applications.

The current service surface is AWS S3.

## Packages

Awskit follows a Cohttp-style package split. Install only the runtime adapter
you need.

| Package | Description |
| --- | --- |
| `awskit` | Pure AWS core: credentials, regions, endpoints, SigV4 signing, retries, request/response types, errors, and the runtime module type. |
| `awskit-unix` | Unix helpers for clocks, environment variables, shared AWS credentials, and config files. |
| `awskit-lwt` | Generic Lwt runtime adapter over a caller-supplied Cohttp Lwt client. |
| `awskit-lwt-unix` | Ready-to-use Lwt + Unix runtime adapter using Cohttp Lwt Unix. |
| `awskit-eio` | Ready-to-use direct-style Eio runtime adapter using Cohttp Eio. |
| `awskit-s3` | Pure AWS S3 core: buckets, objects, multipart upload, presigned URLs, policies, endpoint resolution, and simulator. |
| `awskit-s3-lwt` | S3 adapter over the generic Awskit Lwt runtime. |
| `awskit-s3-lwt-unix` | Ready-to-use S3 client for Lwt + Unix applications. |
| `awskit-s3-eio` | Ready-to-use S3 client for Eio applications. |

The core `awskit` and `awskit-s3` packages do not depend on Unix, Eio, Lwt, or
Cohttp runtime packages. Adapter packages carry those dependencies.

## S3

`awskit-s3` exposes AWS S3 operations for:

- bucket creation, deletion, listing, and configuration;
- object put, get, head, delete, copy, ranges, metadata, tags, and versions;
- multipart upload and managed multipart helpers;
- presigned URLs;
- bucket policies and related XML/JSON wire types;
- S3 endpoint and addressing configuration;
- structured S3 error classifiers;
- deterministic in-memory simulation for tests.

Awskit targets AWS S3 semantics. S3-compatible services such as MinIO are useful
for local contract testing, but provider-specific behavior should stay in tests
unless it matches AWS S3.

## Streaming Contract

Runtime upload bodies carry an upload descriptor with `content_length`,
`payload_hash`, and `replayable` metadata. S3 `PutObject` and `UploadPart`
currently require a known `content_length`; Awskit does not implement
unknown-length SigV4 aws-chunked streaming.

For already-buffered data, prefer string or bytes body helpers. Custom stream
bodies must emit exactly the declared number of bytes, and producer callback
errors are reported as upload failures. Mark custom streams replayable only
when the stream can actually be replayed for a retry.

Response bodies are streaming and scoped to the runtime response callback.
Runtime adapters expose `with_response`; inside that callback, consume bodies
through `with_download_body` or S3 helper APIs such as
`Object.Buffer.get_string ~max_size`. Buffering and draining helpers apply
their documented response-size limits.

## Quick Start

For Eio:

```sh
opam install awskit-s3-eio eio_main
```

```lisp
; dune
(libraries awskit awskit-s3 awskit-s3-eio eio_main)
```

```ocaml
open Eio.Std

let () =
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let credentials =
    Awskit.Credentials.create_exn
      ~access_key_id:"AKIAIOSFODNN7EXAMPLE"
      ~secret_access_key:"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
      ()
  in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  let s3 = Awskit_s3_eio.create ~sw ~env ~region ~credentials () in
  match
    Awskit_s3_eio.Object.Buffer.put_string s3
      ~bucket:"my-bucket"
      ~key:"hello.txt"
      "Hello, S3!"
  with
  | Ok uploaded ->
      Fmt.pr "Uploaded. ETag: %a@."
        (Fmt.option Awskit_s3.Object.Etag.pp)
        uploaded.etag
  | Error error -> Fmt.epr "S3 error: %a@." Awskit_s3.Error.pp error
```

For Lwt + Unix:

```sh
opam install awskit-s3-lwt-unix
```

```lisp
; dune
(libraries awskit awskit-s3 awskit-s3-lwt-unix lwt.unix)
```

```ocaml
open Lwt.Syntax

let run () =
  match Awskit_s3_lwt_unix.create () with
  | Error error -> Lwt_io.eprintf "S3 error: %a\n" Awskit_s3.Error.pp error
  | Ok s3 ->
      let* result =
        Awskit_s3_lwt_unix.Object.Buffer.get_string s3
          ~bucket:"my-bucket"
          ~key:"hello.txt"
          ~max_size:1_048_576L
          ()
      in
      match result with
      | Ok (_info, body) -> Lwt_io.printl body
      | Error error -> Lwt_io.eprintf "S3 error: %a\n" Awskit_s3.Error.pp error
```

When arguments are omitted, the Unix adapter reads standard AWS configuration
sources:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
AWS_REGION
AWS_DEFAULT_REGION
AWS_PROFILE
AWS_SHARED_CREDENTIALS_FILE
AWS_CONFIG_FILE
AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
AWS_CONTAINER_CREDENTIALS_FULL_URI
AWS_CONTAINER_AUTHORIZATION_TOKEN
AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE
```

Pass an explicit `endpoint` when testing against a local service or custom AWS
endpoint.

## Addressing Style

S3 supports virtual-hosted and path-style bucket addressing. Awskit exposes
this as:

```ocaml
type addressing_style = [ `Auto | `Path | `Virtual_hosted ]
```

`Auto` uses virtual-hosted addressing when the bucket and endpoint support it,
and falls back to path-style otherwise. Local test services commonly need
`~addressing_style:`Path`.

## Simulation

The simulator provides deterministic in-memory S3 for unit tests. It does not
require network access or AWS credentials.

```ocaml
let () =
  let credentials =
    Awskit.Credentials.create_exn
      ~access_key_id:"AK"
      ~secret_access_key:"SK"
      ()
  in
  let clock = Awskit_s3.Sim.Clock.create () in
  let store = Awskit_s3.Sim.create_store ~clock () in
  let conn = Awskit_s3.Sim.connect store ~credentials in

  Awskit_s3.Sim.Bucket.create conn ~bucket:"test" () |> ignore;
  Awskit_s3.Sim.Object.Buffer.put_string conn
    ~bucket:"test"
    ~key:"hello"
    "world"
  |> ignore
```

## Development

Install dependencies and run the default build and test suite:

```sh
opam install . --deps-only --with-test
opam exec -- dune build
opam exec -- dune test
```

Optional MinIO contract tests:

```sh
docker compose up -d
opam exec -- dune build @minio-contract
```

The MinIO contract runner defaults to `http://127.0.0.1:9000` with
`minioadmin` credentials from `docker-compose.yml`. Override with:

```text
AWSKIT_S3_MINIO_ENDPOINT
AWSKIT_S3_MINIO_ACCESS_KEY_ID
AWSKIT_S3_MINIO_SECRET_ACCESS_KEY
AWSKIT_S3_MINIO_REGION
```

## Layout

```text
packages/
  awskit/              pure AWS core
    unix/              Unix helpers
    lwt/               generic Lwt runtime
    lwt/unix/          ready-to-use Lwt + Unix runtime
    eio/               ready-to-use Eio runtime
  awskit-s3/           pure AWS S3 core
    sim/               in-memory S3 simulator
    lwt/               S3 over generic Lwt runtime
    lwt/unix/          ready-to-use S3 Lwt + Unix client
    eio/               ready-to-use S3 Eio client
doc/                   odoc landing page
test/                  package tests and MinIO contracts
```

## License

MIT.
