## 0g Fragment Workflow

This Go utility automates the workflow described in the prompt:

- create (or validate) a 4 GB source file
- split it into 10 × 400 MB fragments
- invoke `0g-storage-client` with the correct `--fragment-size` parameter to upload and download each fragment
- merge the downloaded pieces and verify the checksum

### Prerequisites

- Go 1.20+ (tested with Go 1.23)
- `0g-storage-client` available in `PATH` or provide `--client-bin`

### Usage

```powershell
go run . ^
  --source-file data\source.bin ^
  --work-dir workdir ^
  --remote-prefix demo-frag ^
  --upload-extra "--endpoint https://rpc.example --bucket my-bucket" ^
  --download-extra "--endpoint https://rpc.example --bucket my-bucket"
```

Key flags:

- `--fragment-size` (default `400MB`): forwarded to `0g-storage-client`
- `--chunk-count` / `--chunk-size`: defaults to 10 × 400 MB (4 GB total)
- `--skip-upload`, `--skip-download`, `--skip-verify`: useful when testing subsets

Every upload command looks like:

```
0g-storage-client upload --file chunk-XX.bin --remote-name <prefix>-XX.bin --fragment-size 400MB …
```

Downloads mirror the same naming, placing files under `workdir/downloads`. After downloading, the tool merges fragments into `workdir/merged.bin` and compares SHA-256 hashes with the original to guarantee integrity.

Adjust `--upload-extra` and `--download-extra` for authentication or network options required by your 0g deployment.

