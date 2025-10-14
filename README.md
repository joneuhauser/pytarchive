# pytarchive: CLI tool for archiving on LTFS

This tool provides the following features:
- `pytarchive archive`: Archive folders on the network to tape, where the tapes are managed by a tape library (mtx changer), and verifies that everything has been archived
- `pytarchive summary`: Keep track of what is archived on which tape
- `pytarchive restore`: Restore folders from tape
- `pytarchive explore`: Provide read-only access to an archived folder, exported to NFS
- `pytarchive inventory`: Check the subfolders of a folder (e.g. the network drive of user folders) for size and age and sends out an email report.

**Important**: Pytarchive never deletes original files, this must be done manually (safety precaution)

The pytarchive service runs in the background with root permissions, receives commands by UID 0, manages the system resources (tape drive, changer, etc.) and processes the scheduled tasks. 

The pytarchive client is a minimal application that sends commands to the 

# Prerequisites

- A tape library that `mtx` supports
- LTFS must be installed.

# Setup

Consider the following Ansible setup as a guide:

```yaml
- name: Install and setup pytarchive
  hosts: library_host
  become: yes
  gather_facts: no
  tasks:
    - name: Ensure /ltfs exists
      file:
        path: /ltfs
        state: directory
        mode: 0700
    - name: Ensure /etc/pytarchive exists
      file:
        path: /etc/pytarchive
        state: directory
        mode: 0700
    - name: Ensure /var/lib/pytarchive exists
      file: # In this directory the database is stored! Make sure to have a backup of it.
        path: /var/lib/pytarchive
        state: directory
        mode: 0700
    - name: Main configuration file
      copy:
        dest: /etc/pytarchive/pytarchive.conf
        content: |
          [General]
          tape_max_size = 17138671616
          exclude_folders = .local, .cache, .config 
          source_folders = /path/to/user/homes, /another/network/drive # Paths for pytarchive inventory
          scratch_path = /tmp # Path where compressed folders are stored.

          [Device]
          library = /dev/sch0 # the device id of the changer (not the tape drive)
          drive_serial = 10WT012345 # obtain with sudo tapeinfo -f /dev/st0 

          [Export]
          to = 192.168.0.0/16 # Network to export to for pytarchive explore
          settings = ro,fsid=1 # NFS export options
    - name: Create pytarchive logging conf
      copy:
        dest: /etc/pytarchive/logging.conf
        content: |
          # logging.conf 
          [loggers]
          keys=root

          [handlers]
          keys=rotatingFileHandler, emailHandler

          [formatters]
          keys=defaultFormatter

          [logger_root]
          level=INFO
          handlers=rotatingFileHandler, emailHandler

          [handler_consoleHandler]
          class=StreamHandler
          level=DEBUG
          formatter=defaultFormatter
          args=(sys.stdout,)

          [handler_rotatingFileHandler]
          class=logging.handlers.RotatingFileHandler
          level=INFO
          formatter=defaultFormatter
          args=('/var/log/pytarchive.log', 'a', 10485760, 5)

          [handler_emailHandler]
          class=logging.handlers.SMTPHandler
          level=CRITICAL
          formatter=defaultFormatter
          args=('smarthost.example.org', 'pytarchive@example.org', ['it@example.org'], 'pyarchive: Task failed')

          [formatter_defaultFormatter]
          format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
          datefmt=%Y-%m-%d %H:%M:%S
    - name: Clone pytarchive
      git:
        repo: https://github.com/joneuhauser/pytarchive.git
        dest: /opt/pytarchive
        version: main

    - name: Ensure python3-venv is installed
      apt:
        name: python3-venv
        state: present
    - name: Create virtual environment
      command:
        cmd: python3 -m venv /opt/pytarchive/.venv
        creates: /opt/pytarchive/.venv/bin/activate
    - name: Install poetry into virtualenv
      command:
        cmd: /opt/pytarchive/.venv/bin/pip install poetry
    - name: Install pytarchive dependencies
      command:
        cmd: /opt/pytarchive/.venv/bin/poetry install
        chdir: /opt/pytarchive
    - name: Create the systemd service file for pytarchive
      copy:
        dest: /etc/systemd/system/pytarchive.service
        content: |
          [Unit]
          Description=Python-based tape archiving service
          After=network.target

          [Service]
          Type=simple
          User=root
          ExecStart=/opt/pytarchive/.venv/bin/python /opt/pytarchive/pytarchive/service/service.py
          Restart=on-failure

          [Install]
          WantedBy=multi-user.target
      register: pytarchive_service
    - name: Enable and start the pytarchive service
      systemd:
        name: pytarchive
        enabled: yes
        state: started
      when: pytarchive_service.changed
    - name: symlink /opt/pytarchive/pytarchive/client/pytarchive.py to /usr/local/bin/pytarchive
      file:
        src: /opt/pytarchive/pytarchive/client/pytarchive.py
        dest: /usr/local/bin/pytarchive
        state: link
        mode: '0755'
      register: pytarchive_link
```

# Warnings

Do not power off the library without moving the tape out of the drive. I've had tapes being corrupted that way.

When mtx reports an error, take it seriously. Power cycling the library or host probably WILL make it worse.

# Features

Workflow:
  1. Mount a network drive that you want to archive. 
  1. Prepare the directory for archiving: `pytarchive prepare /full/path/to/folder "description"`. `pytarchive` decides whether there are too many individual files (>500k) for LTFS and compresses the folder to the `scratch_folder` if necessary. 
  1. When this is done, archive it (set the correct tape number): `pytarchive archive /full/path/to/folder TPN123L9`
  1. You can restore it with `pytarchive restore ...`
  1. Periorically, look at `pytarchive deleteable` to see what can be deleted. Note that folders are only set to archived after a consistency check, so whatever is listed there should be save to delete.
  1. Periodically, run `pytarchive inventory` to check age and sizes of all folders on the network drives.

All of these commands create work items and are put into a queue. Inspect the queue with `pytarchive queue` 

If a task failed and you need to restart it, run `pytarchive requeue [<task-id>[,<task-id>, ...] [--all]`
The task-id is listed in the `pytarchive queue` command. 

View the current state of the library with `pytarchive summary`.

# Troubleshooting

Look at the logs in `/var/log/pytarchive.log` to check what went wrong. Fix the issue and restart the task. 

If the mtx commands fails, look up the "Request sense code" here: https://www.t10.org/lists/asc-num.htm#ASCD_53

## Common errors

```
Unloading drive 0 into Storage Element 6...mtx: Request Sense: Long Report=yes
mtx: Request Sense: Valid Residual=no
mtx: Request Sense: Error Code=70 (Current)
mtx: Request Sense: Sense Key=Illegal Request
mtx: Request Sense: FileMark=no
mtx: Request Sense: EOM=no
mtx: Request Sense: ILI=no
mtx: Request Sense: Additional Sense Code = 53
mtx: Request Sense: Additional Sense Qualifier = 03
mtx: Request Sense: BPV=no
mtx: Request Sense: Error in CDB=no
mtx: Request Sense: SKSV=no
```

The pytarchive daemon was stopped while a tape was still mounted. Don't do that! 

Quickest fix is usually to mount the tape and then unmount it. **DO NOT POWER CYCLE THE LIBRARY OR YOU MIGHT LOSE DATA!**

