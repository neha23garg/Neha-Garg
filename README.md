File server with a simple read/write interface.

Sever.go: Server can handle four operations 1. Read file 2. Write file 3. Compare and swap file 4. Delete file

Each file is having a version number associated with it. Files are stored in memory and it's optional that each file might have a expiry time(interval in seconds after which the content may not be available).

basic_Test.go: This file contains all test cases(operations) exected on the server and test cases are executed considering concurrency factor.

Build and execute: Browse to the folder containing these files through terminal and type below command in terminal go test

This command will build and execute all the test cases present in basic_Test.go.
