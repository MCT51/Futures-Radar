**READ THIS BEFORE PUSHING OR PULLING**

This repository uses Git large file storage for datasets > 100MB.
Run "git lfs install" in your terminal and everything should work fine.

If you want to push datasets > 100MB, use git lfs track to add specific folders to be pushed to git LFS rather than the standard git.
I have named such folders with -needslfs at the end for clarity.
