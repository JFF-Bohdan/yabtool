# yabtool

![Build status](https://travis-ci.org/JFF-Bohdan/yabtool.svg?branch=master "Build status from Travis CI")

Yet Another Backup Tool - backup tool with configurable flow.

Backup tool with ability to execute different backup flows made with
steps that communicates with each other.

## General description

For example may be used to perform this flow:

- make temporary directory for further activities;
- perform database backup into this folder and also save backup log in same folder;
- calculate hash sum (SHA-256 for example) of backup file and save hashes to file;
- compress folder with 7Z algorithm and encrypting it with AES;
- calculate hash of archive and save near archive;
- upload archive and file with hash into S3 bucket and perform backup rotation.

All supported flows should be described in configuration file (in YAML format).
Yabtool by default provides `config.yaml` file with preconfigured execution
flows (see section below). In case if during execution configuration file
will not be provided, default file will be used.

The flow may be designed with atomic steps that performs single operation
like: making backup, compressing file, etc.

Tool may be used to:

- backup different databases just by replacing step
that performs backup;
- perform backup of folder with files by compressing this folder with
7Z or any other suitable backup tool/archive;
- save result backup file(s) to folder and perform backup file
rotation by any kind of rule: rotate by week of the day, month or any
other suitable rule;
- upload result backup file(s) to FTP/S3 or any other storage and
perform backup file rotation by any kind of rule: rotate by week of
the day, month or any other suitable rule.

## Secrets separation

Many steps may require secrets for their operation, for example:

- passwords for archives generation and validation;
- `aws_access_key_id`, `aws_secret_access_key`  - for AWS API usage

Other secrets also may be required for tool usage. The same configuration
file but with different secrets may be used to perform operations against
different targets. For example: against different databases, on different
hosts, etc.

File with secrets may contain all information required for specific flow and
also override default values for flow steps specified in configuration file.

## Available preconfigured flows

### **`fb7zs3-flow`** - Firebird backup and upload to the S3 with rotation

This flow performs these operations and these steps:

- `mkdir_for_backup` - makes temporary directory for further activities;
- `firebird_backup` - performs Firebird database backup into folder and
also save backup log in same folder;
- `calculate_file_hash_and_save_in_file` - calculates hash sum (SHA-256 by default)
of backup file and save hashes to file near backup file and backup log;
- `7z_compress` - compresses folder using 7Z algorithm and encrypting it with AES;
- `calculate_file_hash_and_save_in_file` - calculates hash of archive and save
near archive;
- `validate_7z_archive` - validates 7Z archive before upload;
- `s3_multipart_upload` - uploads archive and file with hash sum to the S3
bucket and performs backup rotation with these rules:
    - weekly - by using number of week of the year. As a result bucket
    will contain archive for each week for the year;
    - weekdays - by name of weekday. As a result bucket will contain
    backup for each week day and backups for last 7 days;
    - monthly - by name of the month. As a result, bucket will contain
    backup for 1st day of each month (in case if tool will be
    executed each day).
