Setting up Bitbucket on a Workstation
=====================================

Install Git for Windows
or Lowe's Blessed: Links to Enablers (Lowe's scanned Git Client Installers)
Get Eclipse working with Git


Configure Git for the first time
---------------------------------

```bash
git config --global user.name "Firstname Lastname"
git config --global user.email "Firstname.Lastname@Lowes.com"
git config --global core.autocrlf false
#turns off Gits Line Ending Auto-Conversion feature. (avoids warnings message about line ending conversion.)
```


Generate an ssh key from git Gui
Add your ssh key to your bitbucket account

**Note:** The Instructions below were modified from the post repository creation instructions provided by BitBucket.
Working with a repository


### I just want to clone a repository
If you want to simply clone this empty repository then run this command in your terminal.


#### Note :

```bash
${repository-name} needs to be replaced with the url friendly repository name
${project-name} needs to be replaced with the url friendly project name
git clone ssh://git@vmlnxatlstash.lowes.com:42000/${project-name}/${repository-name}.git
```


### My code is ready to be pushed



If you already have code ready to be pushed to this repository then run this in your terminal.

#### create a new folder

```bash
cd new_folder
git clone ssh://git@vmlnxatlstash.lowes.com:42000/${project-name}/${repository-name}.git
#Move files into the new repo
git add --all
git commit -m "Initial Commit"
git push -u origin master
````

### My code is almost ready to be pushed (I want to ignore some files/directories first) - recommended approach


If you already have code, but it has files that don't necessarily need to be tracked in version control.
Such as Large Binary files, or compiled output, then follow this procedure in your terminal.</p>

```bash
cd existing-project<br>
git init
````

add necessary any .gitignore files to your project. then run
**NOTE:** Github has an excellent collection of .gitignore templates

`git add --all --dry-run`

1.  to verify that all of your project files will be added correctly and the correct files ignored. once your .gitignore file is correct:
2.  run the following to:
3.  Add all files
4.  Set the Bitbucket Server Repository
5.  push the changes to the server</p>

```bash
git add --all git commit -m "Initial Commit"
git remote add origin ssh://git@vmlnxatlstash.lowes.com:42000/${project-name}/${repository-name}.git
git push -u origin master
```


#### My code is already tracked by Git


If your code is already tracked by Git then set this repository as your "origin" to push to.

```bash
cd existing-project
git remote set-url origin ssh://git@vmlnxatlstash.lowes.com:42000/${project-name}/${repository-name}.git
git push -u origin master
```

Creating and Working with a Branch
----------------------------------

To Create a New Branch

`git checkout -b NewBranchName`

To push the new Branch to BitBucket and create it there.
Where origin has already been created to point to the the BitBucket Server

`git push -u origin NewBranchName`


Pull Requests
-------------

1. Are Created in the ButBucket UI.
   More Information can be found on Atlassian's Website - Making a Pull Request.
2.  You are not allowed to review your own Pull Requests.
