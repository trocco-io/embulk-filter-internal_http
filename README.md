# Internal Http filter plugin for Embulk
Embulk filter plugin used for internal purposes.

## Overview

* **Plugin type**: filter

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
filters:
  - type: internal_http
    option1: example1
    option2: example2
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
