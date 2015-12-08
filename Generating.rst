Running the Python codegen
==========================

Overview
--------

There are currently two stages of the code generation:

* gRPC/proto codegen
* VGen (Veneer codegen)

Eventually, a third stage of the codegen will perform a three-way merge to
incorporate hand-edits of the generated Veneer file, but this is not currently
done.

Project setup
-------------

Each Python project (e.g., gapi-bigtable-python, gapi-logging-python, etc.)
should initially contain the following:

* A service yaml
* Service proto(s) (note that protos common across APIs live in gapi-core-proto
  and so do not need to be replicated in each project)
* A VGen yaml

Running gRPC/proto codegen
--------------------------

NOTE: A sample script for performing this step is available here:
`run_codegen.sh`_. It should be possible to perform this step by copying
`run_codegen.sh` into your project and changing the hard-coded directories
at the top of the script to point to your proto files.

.. _run_codegen.sh: https://gapi.git.corp.google.com/gapi-bigtable-python/+/master/src/main/run_codegen.sh

This is done with the `protoc` tool. There are two sets of artifacts that
`protoc` must produce:

* A descriptor set
* gRPC codegen output

The descriptor set is a binary file compiled directly from the protos; it is
the input that the Tools Framework uses to build its internal model of the API.

The gRPC codegen is done by using `grpc_python_plugin` (this should have been
installed during the setup of the development environment) with `protoc`. These
are not necessary to run the Veneer codegen, but they are necessary to run its
output.

We do not want to check in the output of `protoc` into GAPI; instead, direct
the output of `protoc` into a directory called `generated`, and `.gitignore`
that directory.

Running VGen
------------

NOTE: the parts of the Tools Framework available on Git-on-Borg can't yet
process all of the sections of the service yaml, so you'll need to edit the
service yaml to remove many of the sections. See `library.yaml`_. for a sample
service yaml that can be processed by the Veneer generator. In particular, the
`authentication` section is not yet available on Git-on-Borg, so the
`_ALL_SCOPES` global variable is the generated code is not currently set
correctly.

.. _library.yaml: https://gapi.git.corp.google.com/gapi-tools/+/master/vgen/src/test/java/io/gapi/vgen/testdata/library.yaml

The Veneer generator can be run through the Java main class in
`CodeGeneratorTool.java`_.

.. _CodeGeneratorTool.java: https://gapi.git.corp.google.com/gapi-tools/+/master/vgen/src/main/java/io/gapi/vgen/CodeGeneratorTool.java

If you have set up the `gapi-tools` to have the correct dependencies per
the `gapi-dev` setup instructions, you can run `CodeGeneratorTool` by passing
in as command-line arguments:

* A base directory in your project
* The descriptor set, relative to the base directory
  (see the "Running gRPC/proto codegen section")
* The service yaml, relative to the base directory
* The VGen yaml, relative to the base directory

The CodeGeneratorTool will produce output in a directory structure derived from
the proto package naming; this directory structure is outputted in the base
directory specified in the command-line args.
