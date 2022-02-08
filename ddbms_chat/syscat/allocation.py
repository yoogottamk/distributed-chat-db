from ddbms_chat.syscat.fragments import FRAGMENTS
from ddbms_chat.models.syscat import Allocation
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.utils import PyQL


ALLOCATION = PyQL(
    [
        Allocation(FRAGMENTS[0], SITES[0]),
        Allocation(FRAGMENTS[1], SITES[1]),
        Allocation(FRAGMENTS[2], SITES[2]),
        Allocation(FRAGMENTS[3], SITES[3]),
        Allocation(FRAGMENTS[4], SITES[0]),
        Allocation(FRAGMENTS[5], SITES[1]),
        Allocation(FRAGMENTS[6], SITES[2]),
        Allocation(FRAGMENTS[7], SITES[3]),
        Allocation(FRAGMENTS[8], SITES[0]),
        Allocation(FRAGMENTS[9], SITES[1]),
        Allocation(FRAGMENTS[10], SITES[2]),
        Allocation(FRAGMENTS[11], SITES[3]),
    ]
)
