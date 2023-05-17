# world.penvm

A file with a `.penvm` suffix is called a PENVM "world" configuration file. It describes the world of networks and targets which are available to PENVM applications.

The configuration file is often, but not necessarily, named according to the application. For example, for the application `myapp`, the name `myapp.penvm` would be used. This naming convention allows for automatic configuration file location: if such a file is found in the working directory at application start time, it should be used as a fallback when no other directives are given.

## Basic Organization

```
meta:
  name: <str>
  description: <str>
  release: <str>
  version: <str>

networks:
  <network-name>:
    boot-concurrency: <int>
    targets: <target-namerange>

  ...

templates:
  <template-name>:
    targets: <target-namerange>
      <target-settings>

  ...

targets:
  <target-name>:
    <target-setttings>
```

Notes:

* The `meta` section contains metadata about the configuration.
* The `networks` section defines networks by name, their targets, and boot-concurrency.
* The `templates` section defines targets using a template format. This is useful for efficiently defining multiple targets (e.g., in clusters) that share the same settings.
* The `targets` section defines individual targets.

A target represents an uninstantiated machine.

A machine is an instantiated target. It often, but not necessarily, corresponds to a single host. A target may be used to instantiate more than one machine instance.

## Target Settings

Target settings are used in the `targets` and `templates` sections. Target settings are organized as:

```
ncores: <int>
memory: <memory>
scheme: ssh
host: <hostname>|<address>
fshome-id: <str>
fsdata-id: <str>
ssl-profile: <str>
```

The `ncores` and `memory` are currently advisory. At some point, they will be the basis for optional target matching based on resource requirements.

The `scheme` specifies how a target can be instantiated. The `ssh` scheme specifies that `ssh` be used to set up machine instances, and is effectively a launcher of the `penvm-server` program.

The `host` specifies either a host name or address.

The `fshome-id` specifies a tag which identifies the home filesystem used by the target. Targets sharing the same `fshome-id` indicate that they share the same home filesystem.

The `fsdata-id` is used similarly to `fshome-id` but for data filesystems.

The `fshome-id` and `fsdata-id` are used when deploying PENVM, itself, and assets to avoid unnecessary copying.

The `ssl-profile` specifies a name associated with an SSL certificate and key pair used for encrypting communication between client and server. The `ssl-profile` name corresponds to the directory `~/.penvm/ssl/<ssl-profile>/`. A different ssl-profile can be used for each target, which may be appropriate when working across different physical networks/domains.

## Examples

### Cluster

`app.penvm`:

```
meta:
  name: app
  description: Cluster setup.
  #release: 0.1.0
  version: 1

networks:
  cluster-a:
    boot-concurrency: 8
    targets: a-[1-64]

  cluster-b:
    boot-concurrency: 8
    targets: b[0-3]-[0-7]

templates:
  a:
    targets: a-[1-64]
      ncores: 8
      memory: 16GB
      scheme: ssh
      host: $target
      fshome-id: home
      fsdata-id: a
      ssl-profile: default

  b:
    targets: b[0-1]-[0-7]
      ncores: 8
      memory: 32GB
      scheme: ssh
      host: $target
      fshome-id: home
      fsdata-id: b
      ssl-profile: default

    targets: b[2-3]-[0-7]
      ncores: 16
      memory: 64GB
      scheme: ssh
      host: $target
      fshome-id: home
      fsdata-id: b
      ssl-profile: default

targets:
  hn-a:
    ncores: 4
    memory: 16GB
    scheme: ssh
    host: hn-a
    fshome-id: home
    fsdata-id: a
    ssl-profile: default

  hn-b:
    ncores: 4
    memory: 16GB
    scheme: ssh
    host: hn-b
    fshome-id: home
    fsdata-id: b
    ssl-profile: default
```

Notes:

* 2 networks are defined: cluster-a, cluster-b.
* cluster-a machines are uniform (same resource configuration).
* cluster-b has two kinds of machines.
* The `$target` in `host` is automatically replaced with the expanded target name when the template is being processed.
* Both cluster-a and cluster-b share the same home filesystem. Headnodes hn-a and hn-b also share the same home filesystem.
* All cluster-a machines share the same data filesystem.
* All cluster-b machines share the same data filesystem.
* All systems use the same ssl-profile.

Although it is possible to specify a network within the application, PENVM applications use the "default" network by default.

So, in the above, a "default" network would need to be specified with the appropriate targets.

Alternatively, `penvm-boot` can take a network name to boot and make it available as the "default". The `PENVM_AUTO_NETWORK` setting would then need to be available to the application environment.

### Simulate a Cluster

When only one machine is available, we can simulate a cluster with the following.

`app.penvm`:

```
meta:
  name: app
  description: Single machine cluster.
  #release: 0.1.0
  version: 1

networks:
  default:
    targets: mach-[0-7]

template:
  mach:
    boot-concurrency: 8
    targets: mach-[0-7]
      ncores: 1
      memory: 4GB
      scheme: ssh
      host: myhost
      fshome-id: home
      fsdata-id: data
      ssl-profile: default
```

Notes:

* 8 targets are defined: mach-0 to mach-7.
* All targets are in the "default" network.
* The `host` is fixed to the host name `myhost`.
* The home and data filesystems of all the targets are the same.

For this to be workable, the host `myhost` must have enough resources to meet the application needs of 8 machine instances.
