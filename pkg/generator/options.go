/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package generator

import (
	"errors"
	"github.com/dustin/go-humanize"
	"math/rand"
	"strconv"
	"strings"
)

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src              func(o Options) (Source, error)
	customPrefix     string
	random           RandomOpts
	csv              CsvOpts
	minSize          int64
	totalSize        int64
	randomPrefix     int
	randSize         bool
	distributeSize   map[uint64]int64 // bytes -> proportion
	distributeSizeV2 []DistributionSize
}

// OptionApplier allows to abstract generator options.
type OptionApplier interface {
	Apply() Option
}

// getSize will return a size for an object.
func (o Options) getSize(rng *rand.Rand) int64 {
	if o.distributeSize != nil {
		n := rng.Intn(100)
		var counter int64
		for _, v := range o.distributeSizeV2 {
			if v.Proportion+counter > int64(n) {
				return int64(v.Size)
			}
			counter += v.Proportion
		}
	}
	if !o.randSize {
		return o.totalSize
	}
	return GetExpRandSize(rng, o.minSize, o.totalSize)
}

func defaultOptions() Options {
	o := Options{
		src:          newRandom,
		totalSize:    1 << 20,
		csv:          csvOptsDefaults(),
		random:       randomOptsDefaults(),
		randomPrefix: 0,
	}
	return o
}

// WithMinMaxSize sets the min and max size of the generated data.
func WithMinMaxSize(min, max int64) Option {
	return func(o *Options) error {
		if min <= 0 {
			return errors.New("WithSize: minSize must be >= 0")
		}
		if max < 0 {
			return errors.New("WithSize: maxSize must be > 0")
		}
		if min > max {
			return errors.New("WithSize: minSize must be < maxSize")
		}
		if o.randSize && max < 256 {
			return errors.New("WithSize: random sized objects should be at least 256 bytes")
		}

		o.totalSize = max
		o.minSize = min
		return nil
	}
}

// WithSize sets the size of the generated data.
func WithSize(n int64) Option {
	return func(o *Options) error {
		if n <= 0 {
			return errors.New("WithSize: size must be > 0")
		}
		if o.randSize && o.totalSize < 256 {
			return errors.New("WithSize: random sized objects should be at least 256 bytes")
		}

		o.totalSize = n
		return nil
	}
}

// WithRandomSize will randomize the size from 1 byte to the total size set.
func WithRandomSize(b bool) Option {
	return func(o *Options) error {
		if o.totalSize > 0 && o.totalSize < 256 {
			return errors.New("WithRandomSize: Random sized objects should be at least 256 bytes")
		}
		o.randSize = b
		return nil
	}
}

// WithCustomPrefix adds custom prefix under bucket where all warp content is created.
func WithCustomPrefix(prefix string) Option {
	return func(o *Options) error {
		o.customPrefix = prefix
		return nil
	}
}

// WithPrefixSize sets prefix size.
func WithPrefixSize(n int) Option {
	return func(o *Options) error {
		if n < 0 {
			return errors.New("WithPrefixSize: size must be >= 0 and <= 16")
		}
		if n > 16 {
			return errors.New("WithPrefixSize: size must be >= 0 and <= 16")
		}
		o.randomPrefix = n
		return nil
	}
}

func WithDistributionSize(d string) Option {
	return func(o *Options) error {
		ds := strings.Split(d, ",")
		if len(ds) == 0 {
			return errors.New("WithDistributionSize: invalid distribution size")
		}
		var counter int64
		dMap := make(map[uint64]int64)
		var err error
		for _, s := range ds {
			ss := strings.Split(s, ":")
			if len(ss) != 2 {
				return errors.New("WithDistributionSize: invalid distribution size")
			}
			var bs uint64
			if bs, err = toSize(ss[0]); err != nil {
				return err
			}
			proportion := ss[1]
			n, err := strconv.Atoi(proportion)
			if err != nil {
				return err
			}
			if n < 0 {
				return errors.New("WithDistributionSize: invalid distribution size")
			}

			dMap[bs] = int64(n)
			counter += int64(n)
			if counter > 100 {
				return errors.New("WithDistributionSize: invalid distribution size, sum of proportion should be <= 100")
			}
		}
		if counter < 100 {
			dMap[2<<20] = dMap[2<<20] + 100 - counter
		}
		o.distributeSize = dMap
		dList := make([]DistributionSize, len(dMap))
		for k, v := range dMap {
			dList = append(dList, DistributionSize{Size: k, Proportion: v})
		}
		o.distributeSizeV2 = dList
		return nil
	}
}

type DistributionSize struct {
	Size       uint64
	Proportion int64
}

// toSize converts a size indication to bytes.
func toSize(size string) (uint64, error) {
	return humanize.ParseBytes(size)
}
