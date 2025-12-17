#!/usr/bin/env bash

set -e

if [ $# -eq 0 ]; then
	echo "Error: At least one argument is required"
	exit 1
fi

git add -A
git status
read -p "Do you want to proceed with the commit? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
	read -p "Enter commit message: " commit_msg
	git commit -m "${*:1}"
	git push
else
	echo "Commit cancelled."
fi

