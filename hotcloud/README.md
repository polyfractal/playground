
### Building Demo

To run the demo, you must have a copy of nightly Rust installed.  [Multirust](https://github.com/brson/multirust) is the
best/easiest way to do this.  After that, clone the repo, cd into the `hotcloud` directory,
build the project with cargo, then run it (prepending `RUST_LOG` if you wish to see logging
during execution)

```
# Git must be installed before you start

$ curl -sf https://raw.githubusercontent.com/brson/multirust/master/blastoff.sh | sh
... multirust installation ...

$ git clone https://github.com/polyfractal/playground/
$ cd playground/hotcloud
$ multirust override nightly
#------ In ubuntu in ec2 add the following ----
$ sudo apt-get install gcc
$ sudo apt-get install libssl-dev
# check details at https://github.com/sfackler/rust-openssl

#----- Ubuntu Addition ended ---- 
$ cargo build
$ RUST_LOG=hotcloud=DEBUG cargo run
```

The demo expects an Elasticsearch node to be available at `localhost:9200`, and will
delete/reset the indices: `data` and `hotcloud`.  All data will be lost if these already
exist!  Do not run this demo on a production cluster :)
