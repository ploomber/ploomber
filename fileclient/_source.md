---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# File client

<!-- start description -->
Upload task's products upon execution (local, S3, GCloud storage)
<!-- end description -->

Run the pipeline:

```sh
ploomber build
```

The pipeline has a `LocalStorageClient` configured; you'll see a copy of the
products in the `backup` directory. See the `pipeline.yaml` to see how to
switch to S3 and Google Cloud Storage.