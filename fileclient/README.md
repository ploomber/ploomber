<!-- start header -->
To run this example locally, [install Ploomber](https://docs.ploomber.io/en/latest/get-started/quick-start.html) and execute: `ploomber examples -n cookbook/file-client`

To start a free, hosted JupyterLab: [![binder-logo](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fcookbook/file-client%252FREADME.ipynb%26branch%3Dmaster)

Found an issue? [Let us know.](https://github.com/ploomber/projects/issues/new?title=cookbook/file-client%20issue)

Have questions? [Ask us anything on Slack.](https://ploomber.io/community/)

For a notebook version (with outputs) of this file, [click here](https://github.com/ploomber/projects/blob/master/cookbook/file-client/README.ipynb)
<!-- end header -->



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