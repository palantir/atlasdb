
                          __        __          ____
        ____  _________  / /_____  / /_  __  __/ __/
       / __ \/ ___/ __ \/ __/ __ \/ __ \/ / / / /_
      / /_/ / /  / /_/ / /_/ /_/ / /_/ / /_/ / __/
     / .___/_/   \____/\__/\____/_.___/\__,_/_/
    /_/


The protocol buffer compiler is not part of our standard build
process. Instead, you must run it whenever you change a .proto file
and check in the generated code. The compile.sh script can help with
that.


______________________________________________________________________
HOW TO USE THE COMPILE SCRIPT

To compile, for instance, the .proto files in atlas/atlas-client,
you do

    proto/compile.sh atlas/atlas-client

This will automatically find all .proto files in
atlas/atlas-client, compile them, and stick the generated code in
a separate project atlas/atlas-client-protobufs. The necessary
config files for Eclipse and buildlib are also generated.

By sticking the generated files in a separate project, it is possible
to convince both Eclipse and buildlib to use slightly different
compiler and linter settings so as not to choke on generated code that
doesn't meet some of the finer points of corporate style.


______________________________________________________________________
GETTING THE RIGHT VERSION OF THE PROTOBUF COMPILER

You should install protobuf 2.6.0


______________________________________________________________________
CONFIGURE ENVIRONMENT FOR SCRIPT

export PROTOC=$HOME/opt/protobuf/bin/protoc  # or location of your protoc


______________________________________________________________________
CONFIGURE ENVIRONMENT FOR SCRIPT

export PROTOC=$HOME/opt/protobuf/bin/protoc  # or location of your protoc


