from unittest.mock import patch, Mock
from pyarchive.service.library import Library
from pyfakefs.fake_filesystem_unittest import Patcher

state_full = """ Storage Changer /dev/sch0:1 Drives, 24 Slots ( 0 Import/Export )
Data Transfer Element 0:Full (Storage Element 24 Loaded):VolumeTag = AAK787L9                       
      Storage Element 1:Full :VolumeTag=AAK788L9                       
      Storage Element 2:Full :VolumeTag=CLN670L1                       
      Storage Element 3:Full :VolumeTag=AAK780L9                       
      Storage Element 4:Full :VolumeTag=AAK783L9                       
      Storage Element 5:Full :VolumeTag=AAK876L9                       
      Storage Element 6:Full :VolumeTag=AAK786L9                       
      Storage Element 7:Full :VolumeTag=AAK789L9                       
      Storage Element 8:Full :VolumeTag=AAK782L9                       
      Storage Element 9:Full :VolumeTag=AAK877L9                       
      Storage Element 10:Full :VolumeTag=AAK878L9                       
      Storage Element 11:Full :VolumeTag=AAK879L9                       
      Storage Element 12:Full :VolumeTag=AAK781L9                       
      Storage Element 13:Full :VolumeTag=AAK790L9                       
      Storage Element 14:Full :VolumeTag=AAK793L9                       
      Storage Element 15:Full :VolumeTag=AAK796L9                       
      Storage Element 16:Full :VolumeTag=AAK799L9                       
      Storage Element 17:Full :VolumeTag=AAK791L9                       
      Storage Element 18:Full :VolumeTag=AAK794L9                       
      Storage Element 19:Full :VolumeTag=AAK797L9                       
      Storage Element 20:Full :VolumeTag=AAK785L9                       
      Storage Element 21:Full :VolumeTag=AAK792L9                       
      Storage Element 22:Full :VolumeTag=AAK795L9                       
      Storage Element 23:Full :VolumeTag=AAK798L9                       
      Storage Element 24:Empty"""

state_empty = """  Storage Changer /dev/sch0:1 Drives, 24 Slots ( 0 Import/Export )
Data Transfer Element 0:Empty
      Storage Element 1:Full :VolumeTag=AAK788L9                       
      Storage Element 2:Full :VolumeTag=CLN670L1                       
      Storage Element 3:Full :VolumeTag=AAK780L9                       
      Storage Element 4:Full :VolumeTag=AAK783L9                       
      Storage Element 5:Full :VolumeTag=AAK876L9                       
      Storage Element 6:Full :VolumeTag=AAK786L9                       
      Storage Element 7:Full :VolumeTag=AAK789L9                       
      Storage Element 8:Full :VolumeTag=AAK782L9                       
      Storage Element 9:Full :VolumeTag=AAK877L9                       
      Storage Element 10:Full :VolumeTag=AAK878L9                       
      Storage Element 11:Full :VolumeTag=AAK879L9                       
      Storage Element 12:Full :VolumeTag=AAK781L9                       
      Storage Element 13:Full :VolumeTag=AAK790L9                       
      Storage Element 14:Full :VolumeTag=AAK793L9                       
      Storage Element 15:Full :VolumeTag=AAK796L9                       
      Storage Element 16:Full :VolumeTag=AAK799L9                       
      Storage Element 17:Full :VolumeTag=AAK791L9                       
      Storage Element 18:Full :VolumeTag=AAK794L9                       
      Storage Element 19:Full :VolumeTag=AAK797L9                       
      Storage Element 20:Full :VolumeTag=AAK785L9                       
      Storage Element 21:Full :VolumeTag=AAK792L9                       
      Storage Element 22:Full :VolumeTag=AAK795L9                       
      Storage Element 23:Full :VolumeTag=AAK798L9                       
      Storage Element 24:Full :VolumeTag=AAK787L9"""


def mock_unload(to_slot):
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = f"Loading drive 0 into Storage Element {to_slot}...done"
    mock_result.stderr = ""
    return mock_result


def mock_load(from_slot):
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = (
        f"Loading media from Storage Element {from_slot} into drive 0...done"
    )
    mock_result.stderr = ""
    return mock_result


def mock_status(state):
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = state
    mock_result.stderr = ""
    return mock_result


# ltfs output for unformatted drive
""" sudo ltfs -o devname=10WT017752 /ltfs
eb340 LTFS14000I LTFS starting, LTFS version 2.5.0.0 (Prelim), log level 2.
eb340 LTFS14058I LTFS Format Specification version 2.4.0.
eb340 LTFS14104I Launched by "ltfs -o devname=10WT017752 /ltfs".
eb340 LTFS14105I This binary is built for Linux (x86_64).
eb340 LTFS14106I GCC version is 11.4.0.
eb340 LTFS17087I Kernel version: Linux version 5.15.0-112-generic (buildd@lcy02-amd64-051) (gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, GNU ld (GNU Binutils for Ubuntu) 2.38) #122-Ubuntu SMP Thu May 23 07:48:21 UTC 2024 i386.
eb340 LTFS17089I Distribution: DISTRIB_ID=Ubuntu.
eb340 LTFS17089I Distribution: PRETTY_NAME="Ubuntu 22.04.4 LTS".
eb340 LTFS14063I Sync type is "time", Sync time is 300 sec.
eb340 LTFS17085I Plugin: Loading "sg" tape backend.
eb340 LTFS17085I Plugin: Loading "unified" iosched backend.
eb340 LTFS14095I Set the tape device write-anywhere mode to avoid cartridge ejection.
eb340 LTFS30209I Opening a device through sg-ibmtape driver (10WT017752).
eb340 LTFS30288I Opening a tape device for drive serial 10WT017752.
eb340 LTFS30250I Opened the SCSI tape device 1.0.0.0 (/dev/sg5).
eb340 LTFS30207I Vendor ID is IBM     .
eb340 LTFS30208I Product ID is ULTRIUM-HH9     .
eb340 LTFS30214I Firmware revision is Q3F5.
eb340 LTFS30215I Drive serial is 10WT017752.
eb340 LTFS30205I TEST_UNIT_READY (0x00) returns -20601.
eb340 LTFS30290I Changer /dev/sg5 isn't reserved from any nodes.
eb340 LTFS30285I The reserved buffer size of 10WT017752 is 1048576.
eb340 LTFS30294I Setting up timeout values from RSOC.
eb340 LTFS17160I Maximum device block size is 1048576.
eb340 LTFS11330I Loading cartridge.
eb340 LTFS30252I Logical block protection is disabled.
eb340 LTFS11332I Load successful.
eb340 LTFS17157I Changing the drive setting to write-anywhere mode.
eb340 LTFS11005I Mounting the volume from device.
eb340 LTFS30252I Logical block protection is disabled.
eb340 LTFS17168E Cannot read volume: medium is not partitioned.
eb340 LTFS14013E Cannot mount the volume from device.
eb340 LTFS30252I Logical block protection is disabled.
"""


@patch("pyarchive.service.library.subprocess.run")
def test_run_command_success(mock_run):
    with Patcher() as patcher:
        patcher.fs.create_file(
            "/etc/pyarchive.conf",
            contents="[Device]\nlibrary = /dev/sch0 \ndrive_serial=10WT017752",
        )

        mock_run.return_value = mock_status(state_full)

        library = Library()

        assert len(library.get_available_tapes()) == 24

        mock_run.assert_called_once_with(
            ["mtx", "-d", "/dev/sch0", "status"], capture_output=True, text=True
        )
        assert library.get_empty_slots() == [24]
        assert library.find_tape("AAK792") == 21
        assert library.find_tape("AAK792L9") == 21
        assert library.find_tape("CLN670L1") == 2


@patch("pyarchive.service.library.subprocess.run")
def test_load_unload(mock_run):
    with Patcher() as patcher:
        patcher.fs.create_file(
            "/etc/pyarchive.conf",
            contents="[Device]\nlibrary = /dev/sch0 \ndrive_serial=10WT017752",
        )

        mock_run.return_value = mock_status(state_empty)

        library = Library()

        library.find_tape("AAK792") == 21
        mock_run.assert_called_with(
            ["mtx", "-d", "/dev/sch0", "status"], capture_output=True, text=True
        )

        mock_run.return_value = mock_load(state_empty)

        library.load_tape("AAK792")
        mock_run.assert_called_with(["mtx", "-d", "/dev/sch0", "load", "21"])

        library.mount_tape()
