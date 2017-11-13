git checkout stable
git submodule foreach git checkout master
git submodule foreach "git rev-parse --verify refs/heads/stable ; if [ $? -eq 0 ]; then git checkout stable ; fi"
git submodule foreach git pull

cls
@echo Current status:
git submodule foreach git branch

pause