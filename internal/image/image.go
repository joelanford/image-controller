package image

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/archive"
	"github.com/google/go-containerregistry/pkg/name"
	containerregistryv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/nlepage/go-tarfs"
)

type mountOptions struct {
	subPathGetter func(containerregistryv1.Image) (string, error)
}

type MountOption func(*mountOptions)

func WithSubPathGetter(getter func(containerregistryv1.Image) (string, error)) MountOption {
	return func(o *mountOptions) {
		o.subPathGetter = getter
	}
}

type Image struct {
	containerregistryv1.Image
}

func Pull(ctx context.Context, reference string, opts ...remote.Option) (*Image, error) {
	ref, err := name.ParseReference(reference)
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %w", err)
	}

	opts = append(opts, remote.WithContext(ctx))
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("error pulling image: %w", err)
	}

	// TODO: Find a more efficient way of tracking the image. We eventually store
	//   the image data into the memory-backed cache
	buf := new(bytes.Buffer)
	if err := tarball.Write(ref, img, buf); err != nil {
		return nil, fmt.Errorf("error writing image to tarball: %w", err)
	}
	img, err = tarball.Image(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("error reading image from tarball: %w", err)
	}
	return &Image{img}, nil
}

func (img *Image) MountTemp(ctx context.Context, tmpDirRoot string, opts ...MountOption) (string, error) {
	mountOpts := mountOptions{}
	for _, o := range opts {
		o(&mountOpts)
	}

	var (
		subPath string
		err     error
	)
	if mountOpts.subPathGetter != nil {
		subPath, err = mountOpts.subPathGetter(img)
		if err != nil {
			return "", fmt.Errorf("failed to get subpath: %w", err)
		}
	}

	layers, err := img.Layers()
	if err != nil {
		return "", fmt.Errorf("failed to get image layers: %w", err)
	}

	if err := os.MkdirAll(tmpDirRoot, 0755); err != nil {
		return "", fmt.Errorf("error creating temporary directory parent: %w", err)
	}
	tmpDir, err := os.MkdirTemp(tmpDirRoot, ".image-mount-")
	if err != nil {
		return "", fmt.Errorf("error creating temporary mount directory: %w", err)
	}

	subPathBase := filepath.Clean(filepath.Base(subPath))
	for _, layer := range layers {
		layerRc, err := layer.Uncompressed()
		if err != nil {
			return tmpDir, fmt.Errorf("error getting uncompressed layer data: %w", err)
		}

		// Apply the layer contents, but keep only the sub path contents. This filter ensures that the files created
		// have the proper UID and GID for the filesystem they will be stored on to ensure no permission errors occur
		// when attempting to create the files.
		_, err = archive.Apply(ctx, tmpDir, layerRc, archive.WithFilter(func(th *tar.Header) (bool, error) {
			th.Uid = os.Getuid()
			th.Gid = os.Getgid()
			if subPath == "" {
				return true, nil
			}
			dir, file := filepath.Split(th.Name)
			dir, file = filepath.Clean(dir), filepath.Clean(file)
			return (dir == "" && file == subPathBase) || strings.HasPrefix(dir, fmt.Sprintf("%s/", subPathBase)), nil
		}))
		if err != nil {
			return tmpDir, fmt.Errorf("error applying layer to archive: %w", err)
		}
	}
	return tmpDir, nil
}

func (img *Image) Mount(ctx context.Context, mountPath string, opts ...MountOption) error {
	tmpDir, err := img.MountTemp(ctx, mountPath, opts...)
	if tmpDir != "" {
		defer os.RemoveAll(tmpDir)
	}
	if err != nil {
		return fmt.Errorf("error mounting image: %w", err)
	}
	if err := os.Rename(tmpDir, mountPath); err != nil {
		return fmt.Errorf("error renaming temporary directory: %w", err)
	}
	return nil
}

func FS(img Image) (fs.FS, error) {
	imgRc := mutate.Extract(img.Image)
	defer imgRc.Close()
	buf, err := io.ReadAll(imgRc)
	if err != nil {
		return nil, fmt.Errorf("error reading image contents: %w", err)
	}
	return tarfs.New(bytes.NewReader(buf))
}
