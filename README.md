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

## Release

Push a `vX.Y.Z` tag (e.g. `v1.0.2`). GitHub Actions builds the gem with that version and publishes it to GitHub Packages.

Since 1.0.2 the gem is built by the `org.embulk.embulk-plugins` Gradle plugin and is published for the `java` platform
(`embulk-filter-internal_http-X.Y.Z-java.gem`). Versions up to 1.0.1 had no platform, so a `Gemfile.lock` that pins this
gem must be re-locked (`bundle lock --add-platform java`) when upgrading.
