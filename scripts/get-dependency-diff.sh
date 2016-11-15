#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo -e "Usage: \033[1;97m$0 <dependency file before> <dependency file after>\033[0m" >&2
    echo "Go to the old commit and run:" >&2
    echo -e "> \033[1;97m./gradlew -q allCompileDeps -Dorg.gradle.parallel=false > before.txt\033[0m" >&2
    echo "Go to the new commit and run:" >&2
    echo -e "> \033[1;97m./gradlew -q allCompileDeps -Dorg.gradle.parallel=false > after.txt\033[0m" >&2
    echo "Then run:" >&2
    echo -e "> \033[1;97mscripts/get-dependency-diff.sh before.txt after.txt\033[0m" >&2
    exit 255
fi

generate_dependency_files() {
    line_no="$(grep -n "^Project " "$1" | cut -d: -f1)"

    read -a LINES<<< $line_no
    LINES+=($(wc -l < "$1"))

    for i in $(seq 1 1 $((${#LINES[@]}-1))); do
        start=${LINES[i - 1]}
        end=$((${LINES[i]}-1))
        file=$(sed -n "${start}p" "$1" | sed 's/^Project ://')
        sed -n "${start},${end}p" "$1" | grep -- "--- " | sed -e "s/^[^a-zA-Z0-9]*//" -e 's/ (\*)$//' -e 's/:\([0-9].*\)\{0,1\} -> /:/' | sort | uniq > "$2/$file.txt"
    done
}

print_header() {
    printf "\033[1;97m@@$1@@\033[0m\n"
}

prepare_diff() {
    diff -U 0 "$1" "$2" | grep -v "^---" | grep -v "^+++" | grep -v "^@@"
}

print_diff() {
    printf "$(echo "$1" | sed -e 's/^-/\\033[91m-/' -e 's/^+/\\033[92m+/' -e 's/$/\\033[0m/')\\n"
}

before_dir=$(mktemp -d)
after_dir=$(mktemp -d)

generate_dependency_files "$1" "$before_dir"
generate_dependency_files "$2" "$after_dir"

files=$(comm -12 <(cd "$before_dir"; find . -type f) <(cd "$after_dir"; find . -type f))
for file in $files; do
    diff_output=$(prepare_diff "$before_dir/$file" "$after_dir/$file")
    if [[ -n "$diff_output" ]]; then
        print_header "$(echo $file | sed -e 's#^\./##' -e 's#\.txt##')"
        print_diff "$diff_output"
        echo
    fi
done


diff_output=$(prepare_diff <(cd "$before_dir"; cat $files | sort | uniq) <(cd "$after_dir"; cat $files | sort | uniq))
if [[ -n "$diff_output" ]]; then
    print_header "========================"
    print_header "OVERALL FOOTPRINT CHANGE"
    print_header "========================"
    print_diff "$diff_output"
    echo
    exit 1
fi
