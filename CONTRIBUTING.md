# How to Contribute

We welcome contributions from the community and are pleased to have them. Please follow this guide when logging issues or making code changes.

## Logging Issues

All issues should be created using the [new issue](https://github.com/drsoares/kafka-mirror-api/issues/new) form. Describe the issue clearly including steps to reproduce if there are any. Also, make sure to indicate the earliest version that has the issue being reported. 

## Patching code

Code changes are welcome and should follow the guidelines bellow.

 * Fork the repository.
 * Fix the issue ensuring that your code follows the style rules. You can assert this by running `mvn clean install`
 * Make commit of logical units.
 * Chack for unnecessary whitespace with `git diff --check` before commiting.
 * Make sure your commit messages are in the proper format.
 * Add tests for your new code ensuring you have close to 100% code coverage.
    * Run `mvn clean install`
    * Pull requests should be done **from** and **to** the master branch.
