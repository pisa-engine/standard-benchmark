[![Build Status](https://travis-ci.com/pisa-engine/standard-benchmark.svg?branch=master)](https://travis-ci.com/pisa-engine/standard-benchmark)
[![Documentation](https://pisa-engine.github.io/standard-benchmark/master/badge.svg)](https://pisa-engine.github.io/standard-benchmark/master/stdbench/)

# Usage

    USAGE:
        standard-benchmark [FLAGS] [OPTIONS] --config-file <config-file>

    FLAGS:
        -h, --help            Prints help information
            --print-stages    Prints all available stages
        -V, --version         Prints version information

    OPTIONS:
            --config-file <config-file>    Configuration file path
            --suppress <suppress>...       A list of stages to suppress

The main settings are read from the configuration file.
Additionally, we can suppress certain stages with `--suppress` option.
Run with `--print-stages` to see all available stages.

# Configuration File

The program takes a path to configuration file in YAML format.
This section describes all recognized top-level sections.

## Working Directory

This is a working directory. Paths in other settings will be resolved
from it, unless they are absolute.

```yaml
workdir: /path/to/workdir
```

## Source

This item defines where the PISA programs will come from.

### Path

Simply defines a `bin` directory where the executables should reside.

```yaml
source:
    type: path
    path: /usr/bin
```

### Git

Defines Git repository and branch. This source will cause for the code
to be cloned to `workdir` and compiled.

```yaml
source:
    type: git
    path: https://github.com/pisa-engine/pisa.git
```

### Docker

**Unimplemented**

```yaml
source:
    type: docker
    tag: latest
```

## Collections

This is a list of all collections to be tested. Each collection has:
- `name` (or type)
- `collection_dir` -- where the collection is stored
- `forward_index` -- the basename of the forward index (optional; default=`workdir`/fwd/`name`)
- `inverted_index` -- the basename of the inverted index (optional; default=`workdir`/inv/`name`)
- `encodings` -- a list of encodings to compress the index to

```yaml
collections:
    - name: wapo
      description: WashingtonPost.v2
      collection_dir: /data/collections/WashingtonPost.v2
      forward_index: fwd/wapo
      inverted_index: inv/wapo
      encodings:
          - block_simdbp
          - block_qmx
```

## Runs

Runs are experiments to run on the collections, once they are indexed.

**Note:** At this point, only evaluating queries is supported, and the
output of `trec_eval` will be printed on the standard output.

```yaml
runs:
    - collection: wapo
      type: evaluate
      topics: /data/collections/WashingtonPost.v2/topics.core18.txt
      qrels: /data/collections/WashingtonPost.v2/qrels.core18.txt
```
