**READ THIS BEFORE PUSHING OR PULLING**

This repository uses Git large file storage for datasets > 100MB.
Run "git lfs install", then "git lfs pull" in your terminal and everything should work fine.

To use anything, run "python -m pip install -r requirements.txt" in your terminal first

To run heatmap demo run: "python Heatmap/heatmaptest.py", then ctrl + left click on http://<ip here>/

If you want to push datasets > 100MB, use git lfs track to add specific folders to be pushed to git LFS rather than the standard git.
I have named such folders with -needslfs at the end for clarity.
