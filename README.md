# PENVM

## What Is PENVM?

PENVM stands for Programmable, Extensible, Network Virtual Machine.

PENVM is a platform which provides the building blocks to easily and efficiently leverage networked systems.

## Why PENVM?

There is no lack of frameworks and tools for working with servers, communicating over the network, and processing. What PENVM does is make this simple and easy to use, reuse, and share.

Low-level concerns with networking and communication are not exposed. Network setup and management is simple. Communication is uncomplicated. And integration within applications is smooth with freedom to apply as needed.

PENVM provides the means to get up to speed quickly with little hassle.

## What Does PENVM Offer?

Just as Python provides building blocks for writing programs, PENVM provides building blocks for building applications that can easily work across machines to work concurrently and/or in parallel.

PENVM provides the necessary low-level functionality for:

* VM management
  * Query VM features
  * Query VM state/information
* VM network management
  * Hi level handling of networks
  * Set up and control networks of VMs
* Inter-VM communication
  * Structured and typed data
  * Message-based communication
  * Message queues
* Session management
  * Message management and isolation
  * Control flow
  * Synchronization
* Process management
  * Multithreading/concurrency
  * Thread creation
  * Thread termination
* Storage management
  * Filesystem operations

Just like a library, this functionality can be trimmed down, enhanced, reused, and *shared*.

A PENVM kernel consists of a collection of operations (ops) that are callable by clients. This is done by passing requests to a VM. A kernel then processes that request and optionally generates a response and makes it available on a queue. On demand, a client retrieves the response.

While the default kernel is for general purpose use, domain-specific kernels can be developed to meet specific needs. Multiple kernels can be used concurrently by the same machine.

## Build and Installation

PENVM is built and installed to a location from which it is then [deployed](#deployment).

Download (to `~/tmp`) from repo:
```bash
mkdir ~/tmp
cd ~/tmp
git clone https://github.com/penvm/penvm.git
```

Build and install to a destination directory (e.g., `~/penvm`):
```bash
cd ~/tmp/penvm
./penvm-build.py ~/penvm
```

## Deployment

Deployment involves putting the necessary libraries and server across the network of machines which will be participating. This may be general, or for specific applications.

### Local

Create a "local" world file (`local.penvm`):
```yaml
meta:
  name: local
  description: Localhost
networks:
  default:
    targets: localhost
targets:
  localhost:
    scheme: ssh
    host: localhost
```

Deploy libraries and `penvm-server`:
```
export PATH=~/penvm/bin:$PATH
penvm-deploy local.penvm
```

### Add an SSL Profile

Create an SSL profile directory (`default`):
```bash
mkdir -p ~/.penvm/ssl/default
chmod go-rwx ~/.penvm/ssl/default
```

Create server SSL certificate and key files (requires a passphrase):
```bash
cd ~/.penvm/ssl/default
openssl req -x509 -newkey rsa:4096 -keyout server.key- -out server.crt -sha256 -days 365
```

Remove passphrase:
```bash
openssl rsa -in server.key- -out server.key
```

### Network

Change (back) to working directory:
```bash
cd ~/tmp/penvm
```

Create a "network" world file (`net.penvm`) for machines using a shared home (be sure to replace "&lt;host> ..."):
```yaml
meta:
  name: net
  description: Network machines
networks:
  default:
    targets: <host> ...
templates:
  default:
    # change <host> ... as appropriate
    targets: <host> ...
    ncores: 1
    memory: 4G
    scheme: ssh
    fshome-id: net
    ssl-profile: default
```

Deploy libraries and `penvm-server`:
```bash
penvm-deploy net.penvm
```

## Test

Warning: There should be no extraneous output to `stdout` when logging in via `ssh`. E.g., shell configuration files should only output to `stderr`, if at all.

Start network (`net.penvm`):
```bash
penvm-boot --verbose --shell /bin/bash net.penvm
```

Start `penvm-connect` (will use `PENVM_AUTO_NETWORK` setting):
```bash
penvm-connect
```

Get a session (with `default` kernel):
```python
m = machines[0]
s = m.get_session()
print(s)
```

List kernel ops (local):
```python
print("\n".join(dir(s.kernel)))
```

Get Machine snapshot:
```python
s.kernel.machine_snapshot()
r = s.get_response()
print(r.payload.dict())
```

Print also as JSON:
```python
print(r.payload.json())
```

Print also as YAML:
```python
print(r.payload.yaml())
```

List kernels:
```python
s.kernel.machine_list_kernels()
r = s.get_response()
print(r.payload.json())
```

Echo:
```python
s.kernel.echo({"hello": "world"})
r = s.get_response()
print(r.payload.json())
```

Get session for each machine:
```python
ss = [m.get_session() for m in machines]
print(ss)
```

Get Machine state from all machines:
```python
[s.kernel.machine_get_state() for s in ss]
rs = [s.get_response() for s in ss]
for r in rs: print(r.payload.json())
```

Close `penvm-connect` shell:
```
exit()
```

Close PENVM boot session:
```
exit
```

## See Also

* Main site: [https://penvm.dev/](https://penvm.dev/).
* APIs: [https://penvm.dev/docs/apis/penvm](https://penvm.dev/docs/apis/penvm).
* Tools: [https://penvm.dev/docs/tools](https://penvm.dev/docs/tools).
* Examples/demos: [https://penvm.dev/docs/examples](https://penvm.dev/docs/examples).
* Details on the world configuration file: [world.penvm-README.md](world.penvm-README.md).
