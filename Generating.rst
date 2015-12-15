Running the Python codegen
==========================

This file documents setup for a Python project containing a generated Veneer
layer, as well as instructions for running the codegen and (optionally) the
generated code.

Overview
--------

There are currently two stages of the code generation:

* gRPC/proto codegen
* VGen (Veneer codegen)

Eventually, a third stage of the codegen will perform a three-way merge to
incorporate hand-edits of the generated Veneer file, but this is not currently
done.

After code generation, to run the generated code, it is necessary to install
the Python `protobuf` and `oauth2client` packages. It is recommended to use
a virtual environment to do so.

Project setup
-------------

Each Python project (e.g., gapi-bigtable-python, gapi-logging-python, etc.)
should initially contain the following:

* A service yaml
* Service proto(s) (note that protos common across APIs live in gapi-core-proto
  and so do not need to be replicated in each project)
* A VGen yaml

Service yaml
^^^^^^^^^^^^

The service yaml is drawn from google3 and is generally located in
google3/google/<API>. Some services may have multiple yaml files. Some sections
of the yaml file that are supported in google3 have not yet been open-sourced;
see `service.proto`_ to check which sections are available. Non-available
sections must be removed; otherwise, the open-source tools will not be able to
parse the yaml.

.. _service.proto: https://gapi.git.corp.google.com/gapi-core-proto/+/master/src/main/proto/google/api/service.proto

Service proto(s)
^^^^^^^^^^^^^^^^

These are located in the same place in google3 as the service yaml. They often
contain Google internal-only comments and fields, which must be sanitized before
open-sourcing. We properly should set up a workflow with the
`PublicProtoGeneratorTool`_ to do this, but if you're working manually, be sure
to strip out fields with internal-only visibility labels, and comments with
internal-only (i.e., `(-- <comment> --)`) tags.

.. _PublicProtoGeneratorTool: https://cs.corp.google.com/#piper///depot/google3/java/com/google/api/tools/framework/tools/publicprotogen/PublicProtoGeneratorTool.java

VGen yaml
^^^^^^^^^

This yaml specifies how the Veneer generator should perform parameter flattening
(not currently used for Python), page streaming, and other configurable options.
See `config.proto`_ for a specification of this file and the options it
provides.

It is advisable (but not required) to split this yaml into a reusable,
language-independent component (e.g., page streaming) and a non-reusable
language-dependent component (e.g., VGen language provider).

.. _config.proto: https://gapi.git.corp.google.com/gapi-tools/+/master/vgen/src/main/proto/io/gapi/vgen/config.proto

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
in the appropriate command-line arguments. Sample usage:

  ::

    CodeGeneratorTool --base=<PATH-TO-GAPI-DEV>/gapi-dev/gapi-<API>-python/ \
    --descriptorSet=generated/_descriptors/<API>.desc \
    --serviceYaml=configs/<API>.yaml \
    --veneerYaml=configs/<API>_veneer.yaml \
    --veneerYaml=configs/<API>_veneer_python.yaml

In this sample usage, `<API>_veneer.yaml` contains language-independent VGen
configuration, and `<API>_veneer_python.yaml` contains Python-specific
configuration.

The CodeGeneratorTool will produce output in a directory structure derived from
the proto package naming; the tool outputs this directory structure to the base
directory specified in the command-line args.

Setting up a virtual environment
--------------------------------

You must run the generated code from an environment that contains the `protobuf`
and `oauth2client` packages. To do so, you can use the `devenv` tox environment
in the gcloud-python-gax project.

  ::

    gcloud-python-gax$ . .tox/develop/bin/activate
