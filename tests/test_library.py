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
