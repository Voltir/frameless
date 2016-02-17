# Frameless

[![Build Status](https://travis-ci.org/adelbertc/frameless.svg?branch=master)](https://travis-ci.org/adelbertc/frameless)
[![Join the chat at https://gitter.im/adelbertc/frameless](https://badges.gitter.im/adelbertc/frameless.svg)](https://gitter.im/adelbertc/frameless?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

**2016-01-06: The Frameless project has lots of discussion happening in the Gitter channel (see above). The project
is not inactive, we are just discussing how we want the library to look moving forward. Please join the Gitter channel
to help!**

Frameless is a way of using [Spark SQL](http://spark.apache.org/sql/) without completely giving up types. The general
idea is having a phantom type that mirrors the value-level computation at the type-level. All heavy lifting is still
done by the original Spark API.

The Frameless project and contributors support the
[Typelevel](http://typelevel.org/) [Code of Conduct](http://typelevel.org/conduct.html) and want all its
associated channels (e.g. GitHub, Gitter) to be a safe and friendly environment for contributing and learning.

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
