# Changelog

All notable changes to this project will be documented in this file.
See [our coding standards][commit-messages] for commit guidelines.

## 0.1.0 (2024-02-12)


### Features

* add new 'sfn list-executions' command ([5adcdda](https://github.com/onebytegone/aws-utils/commit/5adcdda01d7a7b491a613dc13bcbe15522a28f7c))
* add new unified CLI ([1b715a8](https://github.com/onebytegone/aws-utils/commit/1b715a856949f5c8685faccb21a2828556d10a62))
* add sqs oldest-message-report command ([0434658](https://github.com/onebytegone/aws-utils/commit/043465889b27c7d9a6d2eb49ca0bc0d1744c276d))
* initial commit ([c19fb36](https://github.com/onebytegone/aws-utils/commit/c19fb36a1e8798d061ffa055629f9553e488091a))
* make output arg optional when downloading SQS messages ([6e064fe](https://github.com/onebytegone/aws-utils/commit/6e064fe3dc4f776bfb019f849ff424646024e3f9))
* move default output to gitignored folder ([56058e2](https://github.com/onebytegone/aws-utils/commit/56058e29b6bfec446ab99ae8b7e5fff574ad8ff6))
* remove deprecated download-all-sqs-messages script ([b5d1ff6](https://github.com/onebytegone/aws-utils/commit/b5d1ff65085aa59a8a06a78726f8552018d14be6))
* remove script replaced by the native SFN redrive feature ([552b90c](https://github.com/onebytegone/aws-utils/commit/552b90c4262b1cf4cbb7e5df1ee9d895f3b2c79a))
* wait for multiple empty receives ([1718956](https://github.com/onebytegone/aws-utils/commit/17189560949a5eadc3ae0228d133021ee1a53a2e))


### Bug Fixes

* remove unused --dry-run ([0de104b](https://github.com/onebytegone/aws-utils/commit/0de104bdd67f7aa6f4cb3520e769541b9cf0fc77))
* write messages as NDJSON ([60f1afd](https://github.com/onebytegone/aws-utils/commit/60f1afde72ea85bcb5f2229b4fc3fedf8d451b3c))


[commit-messages]: https://github.com/silvermine/silvermine-info/blob/master/commit-history.md#commit-messages
