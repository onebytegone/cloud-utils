# Changelog

All notable changes to this project will be documented in this file.
See [our coding standards][commit-messages] for commit guidelines.

## [0.3.0](https://github.com/onebytegone/cloud-utils/compare/v0.2.1...v0.3.0) (2026-04-19)


### Features

* add --rcu flag to dynamodb query command ([b69ade9](https://github.com/onebytegone/cloud-utils/commit/b69ade96e40df819eae6df0c52a013c21f305a6b))
* add dynamodb query command ([8d71a98](https://github.com/onebytegone/cloud-utils/commit/8d71a98bd85aaa7117e9ed23cc653c4384b0272a))
* add dynamodb segmented scan command ([f26bb35](https://github.com/onebytegone/cloud-utils/commit/f26bb3585124f31bc2209637de0d9979b5974125))


### Bug Fixes

* update aws-sdk-js-v3 monorepo ([706b07f](https://github.com/onebytegone/cloud-utils/commit/706b07faf3a5dee64a2a686f786ffe13de267845))
* update dependency @silvermine/toolbox to v0.7.0 ([82d0d80](https://github.com/onebytegone/cloud-utils/commit/82d0d80aaff53bbf66c77452b4bc0a195263adc1))


## [0.2.0](https://github.com/onebytegone/cloud-utils/compare/v0.1.0...v0.2.0) (2026-04-18)


### Features

* add --region <region> flag to all commands ([5f9fe41](https://github.com/onebytegone/cloud-utils/commit/5f9fe41a10199f1fbd4f239f967ac6d802837262))
* add dyndb bulk-delete command ([dae9cf6](https://github.com/onebytegone/cloud-utils/commit/dae9cf6e8590a36681b1763148e33d56862b5150))
* add lambda bulk-invoke command ([cb81b22](https://github.com/onebytegone/cloud-utils/commit/cb81b22c10491145e4090431becbe950c881c467))
* add lambda invoke command ([0d25e95](https://github.com/onebytegone/cloud-utils/commit/0d25e9567df2c9cfb43c31a85ff977f977784b67))
* add lambda logs helper ([d117c64](https://github.com/onebytegone/cloud-utils/commit/d117c6428314f72a727b226026dd5722e53c7b93))
* add option to ignore empty queues in oldest message report ([f32e5bd](https://github.com/onebytegone/cloud-utils/commit/f32e5bdd4976a5c8aab16ed019739f380e046d56))
* add SQS message to EventBridge event transformer ([85410c8](https://github.com/onebytegone/cloud-utils/commit/85410c80b2029dafe78321555f457439b95d4fd0))
* add step functions start executions command ([eef44e1](https://github.com/onebytegone/cloud-utils/commit/eef44e1e72f846f82f9a58a2214192fa0c7b7e96))
* rebuild CLI using oclif with ESM and tab completion ([e566c08](https://github.com/onebytegone/cloud-utils/commit/e566c088222caef2701c5d8eb5b725feeb918ad0))


### Bug Fixes

* allow more retries when list executions requests are throttled ([26f3bf9](https://github.com/onebytegone/cloud-utils/commit/26f3bf91a8bd988fe6389d2941c1e3277f73a408))
* await writeStream flush before exiting ([d515f7c](https://github.com/onebytegone/cloud-utils/commit/d515f7c651cd2a44c0ac89f14365bf0bd09bcb3a))
* correct time threshold calculations in oldest-message-report ([e6d48eb](https://github.com/onebytegone/cloud-utils/commit/e6d48ebf5c604c88bf30e02d67022dd693e13d2c))
* surface pqueue task failures in bulk commands ([2a7f7a2](https://github.com/onebytegone/cloud-utils/commit/2a7f7a259fcae4d10d0fc25f8582579ca871ddc8))
* use writeStream.end() to flush before closing ([531e0a7](https://github.com/onebytegone/cloud-utils/commit/531e0a7b87145c8540d6557f9ea70ade8c0004d4))


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
