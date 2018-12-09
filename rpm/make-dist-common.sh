#!/usr/bin/env bash

# This script only defines common functions used in all the make-xxx-dist scripts.
# You must set the environment variable LOG=logfile.txt to capture the output.

# The directory of this script.
MAKE_DIST_COMMON_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MAKE_DIST_COMMON_FILE="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"


# ---------------------------------------------------------------------------
# Echo to the $LOG file

function log
{
    echo "$1" | tee -a $LOG
}

# ---------------------------------------------------------------------------
# Run a command and append output to $LOG file which should have already been set.

function run_cmd
{
    local CMD=$1
    #LOG=$2 easier to just use the global $LOG env var set in functions below

    echo " "  >> $LOG
    echo "$CMD" 2>&1 | tee -a $LOG
    eval "$CMD" 2>&1 | tee -a $LOG

    #ret_code=$? this is return code of tee
    local ret_code=${PIPESTATUS[0]}
    if [ $ret_code != 0 ]; then
        printf "Error : [%d] when executing command: '$CMD'\n" $ret_code
        echo "Please see log file: $LOG"
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Ensure that we do not attempt to delete / or /* accidently (in case we rm -rf
# some variable that is not set 'rm -rf ${SOME_BAD_VARIABLE}/*'

function safe_rm_rf
{
    local TARGETS="$@"

    # Come up with a whitelist of targets - only things in /home /mnt /opt and /root are fair game.
    # If you want to delete something outside of these places, then you're on your own.
    local WHITELIST_PREFIXES="/home /mnt /opt /root"

    if [ -z "${TARGETS}" ]; then
        echo "WARNING: attempting delete empty list of items '${TARGETS}' - check caller" >&2
        get_stack >&2
        return 0
    fi

    for TARGET in $TARGETS; do
        TARGET="$(echo "$TARGET" | sed -E 's@/+@/@g')"
        TARGET="$(readlink -m "$TARGET")"

        local TARGET_OK=0
        for WL in $WHITELIST_PREFIXES; do
            # debug...
            #echo "Testing $TARGET against $WL" >&2
            if echo "$TARGET" | grep -E "^$WL" >/dev/null; then
                TARGET_OK=1
                break
            fi
        done

        if [ "$TARGET_OK" != "1" ]; then
            echo "ERROR: Invalid directory to delete ($TARGET)!" >&2
            # run_cmd should handle this...
            #get_stack >&2
            exit 1
        fi
    done

    for TARGET in $TARGETS; do
        rm -rf ${TARGET}
    done
}


# Change to a directory and exit if it failed
function pushd_cmd
{
    local DIR=$1
    pushd $DIR

    if [ $? -ne 0 ] ; then
        echo "ERROR pushd dir '$DIR'" | tee -a $LOG
        exit 1
    fi
}

# Pop from a directory and exit if the pushd stack was empty
function popd_cmd
{
    popd

    if [ $? -ne 0 ] ; then
        echo "ERROR popd from dir '$PWD'" | tee -a $LOG
        exit 1
    fi
}

# Make a tar.gz file and exit on failure. Uses pigz if available for speed.
function run_tar_gz
{
    local TGZ_FILENAME="$1"
    local FILES="$2"

    if which pigz > /dev/null 2>&1 ; then
        run_cmd "tar -cv $FILES | pigz -p 8 > $TGZ_FILENAME"
    else
        run_cmd "tar -czf $TGZ_FILENAME $FILES"
    fi
}


# ---------------------------------------------------------------------------
# Read a conf.ini parameter in the form "KEY SEPARATOR VALUE"
# Usage: get_conf_file_property VARIABLE_NAME "KEY_NAME" "=" conf.ini

function get_conf_file_property
{
    local OUTPUT_VAR_NAME=$1
    local KEY=$2
    local SEPARATOR=$3
    local FILENAME=$4

    # NOTE: grep -E does not support \t directly, but it does support the tab
    #       character, so we spit it out using echo.
    if ! grep -E "^[ $(echo -e '\t')]*$KEY[ $(echo -e '\t')]*$SEPARATOR" "$FILENAME" ; then
        echo "ERROR finding conf param '$KEY' in file '$FILENAME', exiting."
        exit 1
    fi

    local VALUE=$(grep -E "^[ $(echo -e '\t')]*$KEY[ $(echo -e '\t')]*$SEPARATOR" "$FILENAME" | cut -d "$SEPARATOR" -f2 | sed -e 's/^ *//' -e 's/ *$//')
    #echo $VALUE

    eval $OUTPUT_VAR_NAME="'$VALUE'"
}

# ---------------------------------------------------------------------------
# Change a conf.ini parameter in the form "KEY = *" to "KEY = VAL" in FILENAME.
# Usage: change_conf_file_property "KEY_NAME" "=" "NEW_VALUE" conf.ini

function change_conf_file_property
{
    local KEY=$1
    local SEPARATOR=$2
    local NEW_VALUE=$3
    local FILENAME=$4

    # NOTE: grep -E does not support \t directly, but it does support the tab
    #       character, so we spit it out using echo.  Also, -P does support it
    #       but -Pz is currently an unsupported combination.
    #       see... http://savannah.gnu.org/forum/forum.php?forum_id=8477
    # Also verify that it was written since sed always returns 0.
    if [[ $KEY == *"\n"* ]]; then
        run_cmd "sed -i \"N;s@\(^[ \t]*${KEY}[ \t]*${SEPARATOR}\).*@\1${NEW_VALUE}@\" $FILENAME"
        local UPDATED_SETTING=$(sed "$!N;/^[ \t]*${KEY}[ \t]*${SEPARATOR}${NEW_VALUE}/p;D" $FILENAME)
        echo "UPDATED_SETTING=$UPDATED_SETTING"
        if [ -z "$UPDATED_SETTING" ]; then
            echo "ERROR: Unable to update configuration setting $KEY to $NEW_VALUE in $FILENAME."
            exit 1
        fi
        # Cannot use the following as currently grep does not support the combination of
        # -P and -z when searching for beginning or end of line (^ or $).
        #run_cmd "grep -Pzo \"^[ $(echo -e '\t')]*${KEY}[ $(echo -e '\t')]*${SEPARATOR}${NEW_VALUE}\" $FILENAME"
        #run_cmd "grep -Ezo \"^[ $(echo -e '\t')]*${KEY}[ $(echo -e '\t')]*${SEPARATOR}${NEW_VALUE}\" $FILENAME"
    else
        run_cmd "sed -i \"s@\(^[ \t]*${KEY}[ \t]*${SEPARATOR}\).*@\1${NEW_VALUE}@\" $FILENAME"
        run_cmd "grep -E \"^[ $(echo -e '\t')]*${KEY}[ $(echo -e '\t')]*${SEPARATOR}${NEW_VALUE}\" $FILENAME"
    fi
}

function get_file_attrs
{
    local SEARCH_PATH=$1
    local RESULT_FILE=$2
    local IGNORE_FILES="$(echo ${!3})"
    local CONFIG_FILES="$(echo ${!4})"
    local GHOST_FILES="$(echo ${!5})"

    echo > $RESULT_FILE

    pushd_cmd $SEARCH_PATH
        find . -print0 | while read -d '' -r file; do
            local f=$(echo $file | sed 's@^\./@@g')
            #echo "File attrs: $f"
            if [ -L "$file" ]; then
                # symlinks cannot have attr or dir
                echo "%{prefix}/$f" >> "$RESULT_FILE"
            elif [ -d "$file" ]; then
                # Is directory
                echo "%dir %attr(0755, %{owner}, %{user}) \"%{prefix}/$f\"" >> "$RESULT_FILE"
            elif ! echo "$IGNORE_FILES" | grep "$f" > /dev/null ; then
                local FILE_ATTR_PREFIX=""
                if echo "$CONFIG_FILES" | grep "$f" > /dev/null ; then
                    FILE_ATTR_PREFIX='%config(noreplace) '
                elif echo "$GHOST_FILES" | grep "$f" > /dev/null ; then
                    FILE_ATTR_PREFIX='%ghost '
                fi

                # These might be (re)compiled when used, don't let rpm -qV say that they're different, nobody cares.
                if [[ ("$f" == *".pyc") || ("$f" == *".pyo") ]]; then
                    FILE_ATTR_PREFIX='%ghost '
                fi

                if [ -h "$file" ]; then
                    # Symbolic link: warning: Explicit %attr() mode not applicaple to symlink: /opt/gpudb/lib/libjzmq.so.0
                    echo "\"%{prefix}/$f\"" >> "$RESULT_FILE"
                elif [ -x "$file" ]; then
                    # Is executable
                    echo "$FILE_ATTR_PREFIX %attr(0755, %{owner}, %{user}) \"%{prefix}/$f\"" >> "$RESULT_FILE"
                else
                    # Is normal file
                    echo "$FILE_ATTR_PREFIX %attr(0644, %{owner}, %{user}) \"%{prefix}/$f\"" >> "$RESULT_FILE"
                fi
            fi
        done

    popd_cmd

}

# ---------------------------------------------------------------------------
# Verify that the conf files exist within this distribution.

function verify_conf_files
{
    local CONF_FILE="$1"
    local CONF_FILE_PREFIX="$2"
    local INSTALL_PATH="$3"

    echo
    echo "Validating configuration files in '$CONF_FILE' with prefix '${CONF_FILE_PREFIX}' at '${INSTALL_PATH}'..."

    local CONF_FILES=""
    get_relative_conffiles "CONF_FILES" "$CONF_FILE" "$CONF_FILE_PREFIX"

    local MISSING_FILES=0
    local F=""
    for F in $CONF_FILES; do
        INSTALL_FILE="${INSTALL_PATH}$(echo "$F" | sed "s@^${CONF_FILE_PREFIX}@@g")"
        if [ ! -f "${INSTALL_FILE}" ]; then
            echo "Missing configuration file '${INSTALL_FILE}' specified in '${CONF_FILE}'." >&2
            MISSING_FILES=$((MISSING_FILES+1))
        fi
    done

    if [ "$MISSING_FILES" -ne 0 ]; then
        echo "ERROR: Missing ${MISSING_FILES} files specified in '${CONF_FILE}'." >&2
        exit 1
    else
        echo "All configuration files valid."
        echo
    fi
}

function get_relative_conffiles
{
    local OUTPUT_VAR_NAME=$1
    local CONFFILES_FILE=$2
    local STRIP_PREFIX="$3"

    if [ ! -f $CONFFILES_FILE ]; then
        echo "ERROR: conffiles does not exist at ${CONFFILES_FILE}" >&2
        exit 1
    fi

    local F=""
    for F in $(cat $CONFFILES_FILE); do
        F="$(echo "$F" | sed "s@^${STRIP_PREFIX}@@g")"
        eval "$OUTPUT_VAR_NAME=\"$F \$$OUTPUT_VAR_NAME\""
    done
}

# ---------------------------------------------------------------------------
# Get the depenent libs for a specified exe or so.

function get_dependent_libs
{
    local OUTPUT_VAR_NAME=$1
    local EXE_NAME=$2

    local EXE_LIBS=$(ldd $EXE_NAME)
    local EXE_LIBS=$(echo "$EXE_LIBS" | awk '($2 == "=>") && ($3 != "not") && (substr($3,1,3) != "(0x") && (substr($0,length,1) != ":") && ($1" "$2" "$3" "$4 != "not a dynamic executable") {print $3}')
    local EXE_LIBS=$(echo "$EXE_LIBS" | sort | uniq)

    #echo "$EXE_LIBS"

    # Trim out the system libs in /usr/lib* and /lib*, easier to do a positive search.
    local EXE_LIBS=$(echo "$EXE_LIBS" | grep -e "gpudb" -e "/home/" -e "/opt/" -e "/usr/local/" | grep -v "opt/sgi" | grep -v "/usr/local/nvidia/" | grep -v "libnvidia-ml.so")

    eval $OUTPUT_VAR_NAME="'$EXE_LIBS'"
}

# ---------------------------------------------------------------------------
# Utilities to update and fix RPath

function find_elf_files
{
    local DIR="$1"
    local ELF_FILES=`find . -type f -exec file {} \; | grep ELF | awk '{print $1}' | sed 's/:.*$//' | sort | uniq`
    echo "$ELF_FILES"
}

# Update references to a single folder in the RPath.
function fix_rpath
{
    local RPATH_TO_CHANGE="$1"
    local RPATH_TO_CHANGE_TO="$2"

    shift
    shift

    for F in $@; do
        echo "Checking RPATH for: '$F'"

        # "filename.so: RPATH=/home/blah/blah/install/lib"
        # NOTE: You cannot combine these two lines into 'local CHRPATH_OUT=...', as local will eat the return code
        local CHRPATH_OUT
        CHRPATH_OUT="$(chrpath $F)"

        if [ "$?" == 0 ]; then
            # Get the first character pos after "... RPATH="
            local OLD_RPATH=":${CHRPATH_OUT#*=}:"
            local NEW_RPATH="$(echo "${OLD_RPATH}" | sed "s@:${RPATH_TO_CHANGE}:@:${RPATH_TO_CHANGE_TO}:@g")"

            # Clean up extra '/' and ':'
            NEW_RPATH="$(echo $NEW_RPATH | sed 's@:[:]*@:@g')"
            NEW_RPATH="$(echo $NEW_RPATH | sed 's@/[/]*@/@g')"

            NEW_RPATH="$(echo $NEW_RPATH | sed 's@^:@@g')"
            NEW_RPATH="$(echo $NEW_RPATH | sed 's@:$@@g')"
            NEW_RPATH="$(echo $NEW_RPATH | sed 's@/$@@g')"

            if [[ "a$OLD_RPATH" != "a$NEW_RPATH" ]]; then
                echo "Changing rpath for '$F' from '$OLD_RPATH' to '$NEW_RPATH'"
                # Make sure we can modify it
                chmod 'u+rw' "$F"
                chrpath -r "$NEW_RPATH" "$F"

                if [ $? -ne 0 ]; then
                    echo "ERROR changing rpath in '$F' to '$NEW_RPATH'"
                    exit 1
                fi

                echo
            else
                echo "NOT changing rpath for $F, it does not point to $RPATH_TO_CHANGE"
                echo $CHRPATH_OUT
                echo
            fi
        fi
    done
}

# Treat a directory as an installation, and edit the RPath of any files therein
# to relatively point to the <INSTALL_PREFIX>/lib folder.  Also optionally removes a
# DUMMY_RPATH if one was used to build and is provided.
function fix_rpaths
{
    local INSTALL_PREFIX="$1"
    local DUMMY_RPATH="$2"

    echo "----------------------------------------------------------------"
    echo "- fix_rpaths ($INSTALL_PREFIX)"
    if [ -n "$DUMMY_RPATH" ]; then
        echo "              DUMMY_RPATH=$DUMMY_RPATH"
    fi
    echo "----------------------------------------------------------------"

    if [ -d "$INSTALL_PREFIX/lib" ]; then
        pushd $INSTALL_PREFIX/lib
            echo
            local ELF_FILES=$(find_elf_files .)
            if [ -n "$DUMMY_RPATH" ]; then
                fix_rpath "$DUMMY_RPATH" "" $ELF_FILES
            fi

            for f in $ELF_FILES; do
                # Make sure sub dirs have RPATH of '${ORIGIN}/../../lib' pointing back to lib dir.
                local DIR_DEPTH=$(echo $f | grep -o '/' | wc -l)

                if [ $DIR_DEPTH -lt 2 ]; then
                    fix_rpath "$INSTALL_PREFIX/lib" "\${ORIGIN}" $f
                else
                    local DIR_DEPTH_PATH=$(eval printf '../'%.0s {1..$((DIR_DEPTH))})
                    fix_rpath "$INSTALL_PREFIX/lib" "\${ORIGIN}:\${ORIGIN}/${DIR_DEPTH_PATH}lib" $f
                fi
            done
        popd
    fi

    if [ -d "$INSTALL_PREFIX/bin" ]; then
        pushd $INSTALL_PREFIX/bin
            echo
            local ELF_FILES=$(find_elf_files .)
            if [ -n "$DUMMY_RPATH" ]; then
                fix_rpath "$DUMMY_RPATH" "" $ELF_FILES
            fi
            fix_rpath "$INSTALL_PREFIX/lib" "\${ORIGIN}/../lib" $ELF_FILES
        popd
    fi

    if [ -d "$INSTALL_PREFIX/sbin" ]; then
        pushd $INSTALL_PREFIX/sbin
            echo
            local ELF_FILES=$(find_elf_files .)
            if [ -n "$DUMMY_RPATH" ]; then
                fix_rpath "$DUMMY_RPATH" "" $ELF_FILES
            fi
            fix_rpath "$INSTALL_PREFIX/lib" "\${ORIGIN}/../lib" $ELF_FILES
        popd
    fi
}

# ---------------------------------------------------------------------------
# Turns hardcoded "#!/blah/blah/bin/python" to "#!/usr/bin/env python2.7"
# Leaves "#!/usr/bin/env python" as is.
# Fixes all the shebangs python setup.py hardcoded to the abs path to the python exe used for the install.
# http://stackoverflow.com/questions/1530702/dont-touch-my-shebang
function fix_python_shebang
{
    local PYTHON_FILE="$1"

    local PY_SHEBANG="$(head -n 1 $PYTHON_FILE | grep -e '^#\!' | grep 'bin/python')"

    if [[ "a$PY_SHEBANG" != "a" ]]; then
        echo "Fixing shebang for '$PYTHON_FILE' - was '$PY_SHEBANG'"
        sed -i '1s@.*@#\!/usr/bin/env python2.7@' "$PYTHON_FILE"
    fi
}

# ---------------------------------------------------------------------------
# Fix all the shebangs that python setup.py hardcoded to the python exe used for
# the install.  See above.


function fix_python_shebangs
{
    local DIR="$1"
    pushd_cmd $DIR
        for f in `find . -name '*.py'`; do
            fix_python_shebang "$f"
        done
    popd_cmd

    if [ -d "$DIR/bin" ]; then
        pushd_cmd "$DIR/bin"
            for f in $(ls); do
                if [ "$(head -c 2 "$f")" = "#!" ]; then
                    fix_python_shebang "$f"
                fi
            done
        popd_cmd
    fi
}

# ---------------------------------------------------------------------------
# Get OS and distribution info, e.g. .fc20, .el6, etc

function get_os_dist
{
    local OS=""

    if [ $(which rpmbuild) ]; then
        # returns ".fc20" for example
        OS=$(rpmbuild -E "%{dist}" | sed 's/.centos//g' 2> /dev/null)

        # SLES 11.3 has an older rpmbuild that doesn't have %{dist}, dig out the version number
        if [[ "a$OS" == "a%{dist}" ]]; then
            if [ -f /etc/SuSE-brand ]; then
                OS=".sles$(cat /etc/SuSE-release | grep VERSION | grep -oP "[0-9]+")sp$(cat /etc/SuSE-release | grep PATCHLEVEL | grep -oP "[0-9]*")"
            fi
        fi
    elif [ -f /etc/os-release ]; then
        # Ubuntu for example
        OS=".$(grep -E "^ID=" /etc/os-release | cut -d '=' -f 2)$(grep -E "^VERSION_ID=" /etc/os-release | cut -d '=' -f 2 | sed 's/"//g')"
    fi

    if [[ "a$OS" == "a" ]]; then
        # Print error to everywhere, we need to know when this fails.
        echo "Error - unknown OS! Please install 'rpmbuild' or add appropriate code to $MAKE_DIST_COMMON_FILE get_os_dist()"
        >&2 echo "Error - unknown OS! Please install 'rpmbuild' or add appropriate code to $MAKE_DIST_COMMON_FILE get_os_dist()"
        exit 1
    fi

    echo $OS
}

# ---------------------------------------------------------------------------
# Get GIT repo info

# Echos the git "Build Number" the YYYYMMDDHHMMSS of the last check-in.
# Run this function in the git dir.
function get_git_build_number
{
    # Turn '2016-03-17 22:34:47 -0400' into '20160317223447'
    local GIT_BUILD_DATE="$(git --no-pager log -1 --pretty=format:'%ci')"
    echo $GIT_BUILD_DATE | sed 's/-//g;s/://g' | cut -d' ' -f1,2 | sed 's/ //g'
}

# Check if the git repo has modifications, return code 0 means no modifications
# Run this function in the git dir.
function git_repo_is_not_modified
{
    # This line supposedly confirms that all files are actually unchanged
    # vs someone trying to manually force the git index to believe that a
    # file is unchanged (assume-unchanged).  Not sure if it is necessary in our case, but we
    # are leaving it in for now.
    # See:
    # https://github.com/git/git/commit/b13d44093bac2eb75f37be01f0e369290211472c
    # and
    # http://stackoverflow.com/questions/5143795/how-can-i-check-in-a-bash-script-if-my-local-git-repo-has-changes
    # and
    # https://git-scm.com/docs/git-update-index
    git update-index -q --refresh > /dev/null
    # No local changes && no committed changes that have not yet been pushed (diff upsteam vs HEAD returns no results)
    if git diff-index --quiet HEAD -- && [ -z "$(git log @{u}..)" ] ; then
        return 0
    fi

    return 1
}

function git_repo_is_modified
{
    if git_repo_is_not_modified ; then
        return 1
    fi

    return 0
}

# Echos the get_git_build_number with a trailing 'M' if there are local modifications.
# Run this function in the git dir.
function get_git_build_number_with_modifications
{
    local RESULT=$(get_git_build_number)
    if git_repo_is_modified ; then
        RESULT="${RESULT}M"
    fi
    echo $RESULT
}

# Used to compare build numbers.  Build numbers which have a modified flag
# have a higher precedence than build numbers without, but if both build
# numbers are modified are or not, then the numeric portion of the numbers
# are used.
#
function get_maximum_build_number
{
    local A=$1
    local B=$2
    echo "$A" | grep "M" > /dev/null
    local A_HAS_MODS=$?
    echo "$B" | grep "M" > /dev/null
    local B_HAS_MODS=$?
    local A_NUMBER=$(echo "$A" | sed s/M//g)
    local B_NUMBER=$(echo "$B" | sed s/M//g)

    if [ "$A_HAS_MODS" -ne "$B_HAS_MODS" ]; then
        if [ "$A_HAS_MODS" = 0 ]; then
            echo "$A"
        else
            echo "$B"
        fi
    else
        if [ "$A_NUMBER" -ge "$B_NUMBER" ]; then
            echo "$A"
        else
            echo "$B"
        fi
    fi
}

function get_build_number_has_modifications
{
    local BUILD_NUMBER=$1
    return echo "$BUILD_NUMBER" | grep "M"
}

# Echos the current git branch we are on.
function get_git_branch_name
{
    git rev-parse --abbrev-ref HEAD
}

# Gets the "name" of the git repository - the bitbucket.org/gisfederal/NAME.git
function get_git_repo_name
{
    local OUTPUT_VAR_NAME="$1"
    local GIT_REPO_NAME_RESULT=$(git remote show origin -n | grep "Fetch URL:" | sed -r 's#^.*/(.*)$#\1#' | sed 's#.git$##')
    if [ "$?" -ne 0 ]; then
        >&2 echo "ERROR: Unable to retrieve git repo name of $(pwd)"
        exit 1
    fi
    eval "$OUTPUT_VAR_NAME='$GIT_REPO_NAME_RESULT'"
}

# Gets the hash of the latest commit
function get_git_hash
{
    local OUTPUT_VAR_NAME="$1"
    local GIT_HASH_RESULT=$(git --no-pager log --format=%H -1)
    if [ "$?" -ne 0 ]; then
        >&2 echo "ERROR: Unable to retrieve git hash in $(pwd)"
        exit 1
    fi
    eval $OUTPUT_VAR_NAME="$GIT_HASH_RESULT"
}

# Gets the root folder of the git repository.
function get_git_root_dir
{
    local OUTPUT_VAR_NAME="$1"
    local GIT_ROOT_DIR_RESULT=$(git rev-parse --show-toplevel)
    if [ "$?" -ne 0 ]; then
        >&2 echo "ERROR: Unable to get git root directory of $(pwd)"
        exit 1
    fi
    eval $OUTPUT_VAR_NAME="$GIT_ROOT_DIR_RESULT"
}

# Gets all RPM dependencies for libraries and executables - used for creating
# the "requires" statement of an RPM spec file.
function get_rpm_dependencies
{
    local VERBOSE=0

    if [[ "a$1" == "a-v" ]]; then
        VERBOSE=1
        shift
    fi

    local LIB_NAME=$*

    # Only print the rpm name w/o the version so it will work on different distros/version.
    local RPM_QUERY_FORMAT="--qf %{NAME}\\n"

    # Note that "ldd obj" shows all required dependencies, "readelf -d obj" only direct dependencies.

    # See ldd_used_unused_libs.sh (these two calls should be identical)
    local ALL_LIBS=$(ldd $LIB_NAME)
    ALL_LIBS=$(echo "$ALL_LIBS" | awk '($2 == "=>") && ($3 != "not") && (substr($3,1,3) != "(0x") && (substr($0,length,1) != ":") && ($1" "$2" "$3" "$4 != "not a dynamic executable") {print $3}')
    ALL_LIBS=$(echo "$ALL_LIBS" | sort | uniq)

    #echo "$ALL_LIBS" | awk '{print "A: " $0}'

    if [[ "$VERBOSE" == "0" ]]; then
        #echo -e "- ALL RPMs $LIB_NAME links to (direct dependencies only)\n"
        local RPM_FILES=$(for f in $ALL_LIBS; do rpm ${RPM_QUERY_FORMAT} -qf $f; done | sort | uniq)
        echo "$RPM_FILES" | awk '{print $0}'
    else
        #echo -e "- ALL RPMs $LIB_NAME links to (direct dependencies only)\n"
        # Note: no uniq, since we want to display all the original libs
        local RPM_FILES=$(for f in $ALL_LIBS; do RPM_NAME=`rpm ${RPM_QUERY_FORMAT} -qf $f`; echo "$RPM_NAME => $f"; done | sort)
        echo "$RPM_FILES" | awk '{print $0}'
    fi
}

# Gets the version stored in the VERSION file of the repository
function get_version
{
    local OUTPUT_VAR_NAME="$1"

    # check local dir first, in case we are in a project where there are
    # multiple VERSION files and/or a non-standard location.
    local VERSION_FILE="$(pwd)/VERSION"
    # If that cannot be found, then look in the root dir for this repo.
    if [ ! -f $VERSION_FILE ]; then
        local GIT_ROOT_DIR=""
        get_git_root_dir "GIT_ROOT_DIR"
        local VERSION_FILE="$GIT_ROOT_DIR/VERSION"
        if [ ! -f $VERSION_FILE ]; then
            >&2 echo "ERROR: Unable to locate version file in $(pwd)"
            exit 1
        fi
    fi
    get_conf_file_property VERSION_INFO_MAJOR_VERSION "MAJOR" "=" $VERSION_FILE > /dev/null
    get_conf_file_property VERSION_INFO_MINOR_VERSION "MINOR" "=" $VERSION_FILE > /dev/null
    get_conf_file_property VERSION_INFO_REVISION "REVISION" "=" $VERSION_FILE > /dev/null

    eval $OUTPUT_VAR_NAME="$VERSION_INFO_MAJOR_VERSION.$VERSION_INFO_MINOR_VERSION.$VERSION_INFO_REVISION"
}

# Appends the version, latest git hash and branch name to the specified file.
function get_version_info
{
    local OUTPUT_VAR_NAME="$1"

    local GET_VERSION_INFO_REPO_NAME=""
    get_git_repo_name "GET_VERSION_INFO_REPO_NAME"

    local GET_VERSION_INFO_VERSION=""
    get_version "GET_VERSION_INFO_VERSION"

    local GET_VERSION_INFO_HASH=""
    get_git_hash "GET_VERSION_INFO_HASH"

    local GET_VERSION_INFO_BRANCH="$(get_git_branch_name)"

    local RESULT="${GET_VERSION_INFO_REPO_NAME}_VERSION=$GET_VERSION_INFO_VERSION-$(get_git_build_number_with_modifications)"
    local RESULT=$(printf "$RESULT\n${GET_VERSION_INFO_REPO_NAME}_HASH=$GET_VERSION_INFO_HASH")
    local RESULT=$(printf "$RESULT\n${GET_VERSION_INFO_REPO_NAME}_BRANCH=$GET_VERSION_INFO_BRANCH\n")

    #>&2 echo "VERSION INFO = $RESULT"
    #>&2 echo "OUTPUT_VAR_NAME=$OUTPUT_VAR_NAME"
    eval "$OUTPUT_VAR_NAME=\"$RESULT\""
}

# Returns 0 (success) if the provided version info contains a line
# that matches *_VERSION=*M
function version_info_contains_modification
{
    if echo "$1" | grep "_VERSION" | grep "M$" > /dev/null; then
        return 0
    fi
    return 1
}

# Returns 0 (success) if the provided version info does not contain a line
# that matches *_VERSION=*M
function version_info_does_not_contain_modification
{
    if version_info_contains_modification "$1"; then
        return 1
    fi
    return 0
}
