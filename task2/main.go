package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultChunkCount = 10
	defaultChunkSize  = 400 * 1024 * 1024 // 400MB
)

type config struct {
	sourceFile    string
	workDir       string
	chunkDir      string
	downloadDir   string
	mergedFile    string
	chunkSize     int64
	chunkCount    int
	clientBin     string
	remotePrefix  string
	fragmentSize  string
	uploadExtra   []string
	downloadExtra []string
	skipUpload    bool
	skipDownload  bool
	skipVerify    bool
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := parseFlags()
	if err := run(cfg); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
}

func parseFlags() config {
	var (
		sourceFile    = flag.String("source-file", filepath.Join("data", "source.bin"), "path to the 4GB source file (created if missing)")
		workDir       = flag.String("work-dir", "workdir", "base working directory")
		clientBin     = flag.String("client-bin", "0g-storage-client", "path to 0g-storage-client executable")
		remotePrefix  = flag.String("remote-prefix", "four-gig-demo", "prefix used when naming remote fragments")
		fragmentSize  = flag.String("fragment-size", "400MB", "fragment size passed to 0g-storage-client")
		uploadExtra   = flag.String("upload-extra", "", "extra arguments appended to upload commands (space separated)")
		downloadExtra = flag.String("download-extra", "", "extra arguments appended to download commands (space separated)")
		chunkCount    = flag.Int("chunk-count", defaultChunkCount, "number of fragments to create")
		chunkSize     = flag.Int64("chunk-size", defaultChunkSize, "size for each fragment in bytes")
		skipUpload    = flag.Bool("skip-upload", false, "skip invoking 0g-storage-client upload commands")
		skipDownload  = flag.Bool("skip-download", false, "skip invoking 0g-storage-client download commands")
		skipVerify    = flag.Bool("skip-verify", false, "skip checksum comparison between source and merged files")
	)

	flag.Parse()

	work := filepath.Clean(*workDir)
	return config{
		sourceFile:    filepath.Clean(*sourceFile),
		workDir:       work,
		chunkDir:      filepath.Join(work, "chunks"),
		downloadDir:   filepath.Join(work, "downloads"),
		mergedFile:    filepath.Join(work, "merged.bin"),
		chunkSize:     *chunkSize,
		chunkCount:    *chunkCount,
		clientBin:     *clientBin,
		remotePrefix:  *remotePrefix,
		fragmentSize:  *fragmentSize,
		uploadExtra:   splitArgs(*uploadExtra),
		downloadExtra: splitArgs(*downloadExtra),
		skipUpload:    *skipUpload,
		skipDownload:  *skipDownload,
		skipVerify:    *skipVerify,
	}
}

func run(cfg config) error {
	if cfg.chunkCount <= 0 {
		return errors.New("chunk-count must be positive")
	}
	if cfg.chunkSize <= 0 {
		return errors.New("chunk-size must be positive")
	}
	totalSize := int64(cfg.chunkCount) * cfg.chunkSize

	if err := os.MkdirAll(filepath.Dir(cfg.sourceFile), 0o755); err != nil {
		return fmt.Errorf("creating source dir: %w", err)
	}
	for _, dir := range []string{cfg.workDir, cfg.chunkDir, cfg.downloadDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("creating work dir %s: %w", dir, err)
		}
	}

	if err := ensureSourceFile(cfg.sourceFile, totalSize); err != nil {
		return fmt.Errorf("ensuring source file: %w", err)
	}

	chunks, err := splitFile(cfg.sourceFile, cfg.chunkDir, cfg.chunkSize, cfg.chunkCount)
	if err != nil {
		return fmt.Errorf("splitting file: %w", err)
	}
	log.Printf("created %d fragments under %s", len(chunks), cfg.chunkDir)

	meta := makeChunkMeta(cfg.remotePrefix, chunks)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
	defer cancel()

	if !cfg.skipUpload {
		if err := ensureBinary(cfg.clientBin); err != nil {
			return err
		}
		if err := uploadChunks(ctx, cfg, meta); err != nil {
			return fmt.Errorf("uploading fragments: %w", err)
		}
	} else {
		log.Println("skip-upload is set; not calling 0g-storage-client upload")
	}

	var downloaded []chunkDescriptor
	if cfg.skipDownload {
		log.Println("skip-download is set; assuming remote fragments already retrieved")
		downloaded = meta
	} else {
		if err := ensureBinary(cfg.clientBin); err != nil {
			return err
		}
		downloaded, err = downloadChunks(ctx, cfg, meta)
		if err != nil {
			return fmt.Errorf("downloading fragments: %w", err)
		}
	}

	merged, err := mergeChunks(downloaded, cfg.mergedFile)
	if err != nil {
		return fmt.Errorf("merging fragments: %w", err)
	}
	log.Printf("merged file created at %s", merged)

	if cfg.skipVerify {
		log.Println("skip-verify is set; checksum comparison skipped")
		return nil
	}

	srcHash, err := fileHash(cfg.sourceFile)
	if err != nil {
		return fmt.Errorf("hashing source file: %w", err)
	}
	dstHash, err := fileHash(merged)
	if err != nil {
		return fmt.Errorf("hashing merged file: %w", err)
	}
	if srcHash != dstHash {
		return fmt.Errorf("checksum mismatch: source %s, merged %s", srcHash, dstHash)
	}
	log.Printf("checksum verified: %s", srcHash)
	return nil
}

func ensureSourceFile(path string, size int64) error {
	info, err := os.Stat(path)
	if err == nil {
		if info.Size() != size {
			return fmt.Errorf("source file exists but size mismatch: want %d, got %d", size, info.Size())
		}
		log.Printf("source file already present at %s (%d bytes)", path, size)
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}

	log.Printf("creating sparse file at %s (%d bytes)", path, size)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return nil
}

func splitFile(source, outDir string, chunkSize int64, chunkCount int) ([]string, error) {
	file, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var paths []string
	bufferSize := int64(8 * 1024 * 1024)
	buf := make([]byte, bufferSize)

	for i := 0; i < chunkCount; i++ {
		target := filepath.Join(outDir, fmt.Sprintf("chunk-%02d.bin", i))
		out, err := os.Create(target)
		if err != nil {
			return nil, err
		}

		var written int64
		for written < chunkSize {
			toRead := bufferSize
			if remaining := chunkSize - written; remaining < bufferSize {
				toRead = remaining
			}
			n, readErr := file.Read(buf[:toRead])
			if n > 0 {
				if _, err := out.Write(buf[:n]); err != nil {
					out.Close()
					return nil, err
				}
				written += int64(n)
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				out.Close()
				return nil, readErr
			}
		}
		out.Close()
		paths = append(paths, target)
	}

	return paths, nil
}

type chunkDescriptor struct {
	localPath  string
	remoteName string
}

func makeChunkMeta(prefix string, localPaths []string) []chunkDescriptor {
	meta := make([]chunkDescriptor, len(localPaths))
	for i, p := range localPaths {
		meta[i] = chunkDescriptor{
			localPath:  p,
			remoteName: fmt.Sprintf("%s-%02d.bin", prefix, i),
		}
	}
	return meta
}

func uploadChunks(ctx context.Context, cfg config, chunks []chunkDescriptor) error {
	for _, ch := range chunks {
		args := append([]string{
			"upload",
			"--file", ch.localPath,
			"--remote-name", ch.remoteName,
			"--fragment-size", cfg.fragmentSize,
		}, cfg.uploadExtra...)
		log.Printf("uploading %s -> %s", ch.localPath, ch.remoteName)
		if err := runClient(ctx, cfg.clientBin, args); err != nil {
			return err
		}
	}
	return nil
}

func downloadChunks(ctx context.Context, cfg config, chunks []chunkDescriptor) ([]chunkDescriptor, error) {
	var result []chunkDescriptor
	for _, ch := range chunks {
		target := filepath.Join(cfg.downloadDir, filepath.Base(ch.remoteName))
		args := append([]string{
			"download",
			"--remote-name", ch.remoteName,
			"--output", target,
			"--fragment-size", cfg.fragmentSize,
		}, cfg.downloadExtra...)
		log.Printf("downloading %s -> %s", ch.remoteName, target)
		if err := runClient(ctx, cfg.clientBin, args); err != nil {
			return nil, err
		}
		result = append(result, chunkDescriptor{
			localPath:  target,
			remoteName: ch.remoteName,
		})
	}
	return result, nil
}

func mergeChunks(chunks []chunkDescriptor, target string) (string, error) {
	out, err := os.Create(target)
	if err != nil {
		return "", err
	}
	defer out.Close()

	for _, ch := range chunks {
		in, err := os.Open(ch.localPath)
		if err != nil {
			return "", err
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			return "", err
		}
		in.Close()
	}
	return target, nil
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ensureBinary(bin string) error {
	if _, err := exec.LookPath(bin); err != nil {
		return fmt.Errorf("cannot find %s: %w", bin, err)
	}
	return nil
}

func runClient(ctx context.Context, bin string, args []string) error {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func splitArgs(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	return strings.Fields(input)
}
