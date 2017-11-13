git checkout master
git submodule foreach git checkout master
git submodule foreach git fetch

cls
@echo Current status:
git submodule foreach git branch

pause