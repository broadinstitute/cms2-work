"""Miscellaneous utilities, not specific to any workflow"""

import base64
import collections
import contextlib
import datetime
import errno
import gzip
import io
import itertools
import json
import logging
import multiprocessing
import os
import os.path
import re
import shutil
import subprocess
import sys
import tempfile
import time

# third-party imports
import dominate
import dominate.tags
import dominate.util
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

MAX_INT32 = (2 ** 31)-1

def dump_file(fname, value):
    """store string in file"""
    with open(fname, 'w')  as out:
        out.write(str(value))

def pretty_print_json(json_val, sort_keys=True):
    """Return a pretty-printed version of a dict converted to json, as a string."""
    return json.dumps(json_val, indent=4, separators=(',', ': '), sort_keys=sort_keys)

def write_json(fname, json_val):
    dump_file(fname=fname, value=pretty_print_json(json_val))

def _load_dict_sorted(d):
    return collections.OrderedDict(sorted(d.items()))

def _json_loads(s):
    return json.loads(s.strip(), object_hook=_load_dict_sorted, object_pairs_hook=collections.OrderedDict)

def _json_loadf(fname):
    return _json_loads(slurp_file(fname))


def slurp_file(fname, maxSizeMb=50):
    """Read entire file into one string.  If file is gzipped, uncompress it on-the-fly.  If file is larger
    than `maxSizeMb` megabytes, throw an error; this is to encourage proper use of iterators for reading
    large files.  If `maxSizeMb` is None or 0, file size is unlimited."""
    fileSize = os.path.getsize(fname)
    if maxSizeMb  and  fileSize > maxSizeMb*1024*1024:
        raise RuntimeError('Tried to slurp large file {} (size={}); are you sure?  Increase `maxSizeMb` param if yes'.
                           format(fname, fileSize))
    with open_or_gzopen(fname) as f:
        return f.read()

def open_or_gzopen(fname, *opts, **kwargs):
    mode = 'r'
    open_opts = list(opts)
    assert type(mode) == str, "open mode must be of type str"

    # 'U' mode is deprecated in py3 and may be unsupported in future versions,
    # so use newline=None when 'U' is specified
    if len(open_opts) > 0:
        mode = open_opts[0]
        if sys.version_info[0] == 3:
            if 'U' in mode:
                if 'newline' not in kwargs:
                    kwargs['newline'] = None
                open_opts[0] = mode.replace("U","")

    # if this is a gzip file
    if fname.endswith('.gz'):
        # if text read mode is desired (by spec or default)
        if ('b' not in mode) and (len(open_opts)==0 or 'r' in mode):
            # if python 2
            if sys.version_info[0] == 2:
                # gzip.open() under py2 does not support universal newlines
                # so we need to wrap it with something that does
                # By ignoring errors in BufferedReader, errors should be handled by TextIoWrapper
                return io.TextIOWrapper(io.BufferedReader(gzip.open(fname)))

        # if 't' for text mode is not explicitly included,
        # replace "U" with "t" since under gzip "rb" is the
        # default and "U" depends on "rt"
        gz_mode = str(mode).replace("U","" if "t" in mode else "t")
        gz_opts = [gz_mode]+list(opts)[1:]
        return gzip.open(fname, *gz_opts, **kwargs)
    else:
        return open(fname, *open_opts, **kwargs)

def available_cpu_count():
    """
    Return the number of available virtual or physical CPUs on this system.
    The number of available CPUs can be smaller than the total number of CPUs
    when the cpuset(7) mechanism is in use, as is the case on some cluster
    systems.

    Adapted from http://stackoverflow.com/a/1006301/715090
    """

    cgroup_cpus = MAX_INT32
    try:
        def get_cpu_val(name):
            return float(slurp_file('/sys/fs/cgroup/cpu/cpu.'+name).strip())
        cfs_quota = get_cpu_val('cfs_quota_us')
        if cfs_quota > 0:
            cfs_period = get_cpu_val('cfs_period_us')
            _log.debug('cfs_quota %s, cfs_period %s', cfs_quota, cfs_period)
            cgroup_cpus = max(1, int(cfs_quota / cfs_period))
    except Exception as e:
        pass

    proc_cpus = MAX_INT32
    try:
        with open('/proc/self/status') as f:
            status = f.read()
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$', status)
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                proc_cpus = res
    except IOError:
        pass

    _log.debug('cgroup_cpus %d, proc_cpus %d, multiprocessing cpus %d',
               cgroup_cpus, proc_cpus, multiprocessing.cpu_count())
    return min(cgroup_cpus, proc_cpus, multiprocessing.cpu_count())

@contextlib.contextmanager
def timer(msg):
    _log.debug(f'BEG: {msg}')
    beg_time = time.time()
    try:
        yield
    finally:
        _log.debug(f'END: {msg} took {time.time()-beg_time:.2f}s')

def chk(cond, msg='error'):
    if not cond:
        raise RuntimeError(f'ERROR: {msg}')

def mkdir_p(dirpath):
    ''' Verify that the directory given exists, and if not, create it.
    '''
    try:
        os.makedirs(dirpath)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dirpath):
            pass
        else:
            raise

def string_to_file_name(string_value, file_system_path=None, length_margin=0):
    """Constructs a valid file name from a given string, replacing or deleting invalid characters.
    If `file_system_path` is given, makes sure the file name is valid on that file system.
    If `length_margin` is given, ensure a string that long can be added to filename without breaking length limits.

    From https://github.com/broadinstitute/viral-core/blob/v2.1.9/util/file.py#L742
    """
    replacements_dict = {
        "\\": "-", # win directory separator
        "/": "-", # posix directory separator
        os.sep: "-", # directory separator
        "^": "_", # caret
        "&": "_and_", # background
        "\"": "", # double quotes
        r"'": "", # single quotes
        r":": "_", # colon (problem for ntfs)
        r" ": "_", # spaces
        r"|": "-", # shouldn't confuse a vertical bar for a shell pipe
        r"!": ".", # not a bash operator
        r";": ".", # not a terminator
        r"?": "_", # could be mistaken for a wildcard
        r"*": "_", # could be mistaken for a wildcard
        r"`": "_", # no subshells
        r" -": "_-", # could be mistaken for an argument
        r" --": "_--", # could be mistaken for an argument
        r">": "_", # no redirect chars
        r"<": "_", # no redirect chars
        r"(": "__", # special character
        r")": "__", # special character
        r"\\x": "_", # hex char
        r"\\o": "_", # octal char
        #r"\\u": "", # unicode char
        #"": "", # other illegal strings to replace
    }

    # group of ascii control and non-printable characters
    control_chars = ''.join( map(chr, list(range(0,32)) + list(range(127,160)) ) )
    control_char_re = re.compile('[%s]' % re.escape(control_chars))
    string_value = control_char_re.sub("_", string_value)

    # replacements from the dictionary above
    strs_to_replace_re = re.compile(r'|'.join(re.escape(key) for key in replacements_dict.keys()))
    string_value = strs_to_replace_re.sub(lambda x: replacements_dict.get(x.group(), "_"), string_value)

    # condense runs of underscores
    double_underscore_re = re.compile(r'_{2,}')
    string_value = double_underscore_re.sub("_", string_value)

    # condense runs of dashes
    double_dash_re = re.compile(r'-{2,}')
    string_value = double_dash_re.sub("-", string_value)

    # remove leading or trailing periods (no hidden files (*NIX) or missing file extensions (NTFS))
    string_value = string_value.strip(".")

    # comply with file name length limits
    if file_system_path is not None:
        max_len = max(1, max_file_name_length(file_system_path) - length_margin)
        string_value = string_value[:max_len]
        while len(string_value.encode('utf-8')) > max_len:
            string_value = string_value[:-1]

    # ensure all the character removals did not make the name empty
    string_value = string_value or '_'

    return string_value

def run(cmd):
    _log.debug(f'Running command: {cmd}')
    beg_time = time.time()
    try:
        subprocess.check_call(cmd, shell=True)
        _log.debug(f'Succeeded command ({time.time()-beg_time:.2f}s): {cmd}')
    except Exception as e:
        _log.warning(f'Failed command ({time.time()-beg_time:.2f}s): {cmd} with exception {e}')
        raise

def get_dx_project(project_name_or_id):
    '''Try to find the DNANexus project with the given name or id.  Returns a DXProject.'''

    if project_name_or_id.endswith(':'):
        project_name_or_id = project_name_or_id[:-1]

    # First, see if the project is a project-id.
    if project_name_or_id.startswith('project-'):
        try:
            project = dxpy.DXProject(project_name_or_id)
            return project
        except dxpy.DXError:
            pass

    project = list(dxpy.find_projects(name=project_name_or_id, return_handler=True, level="VIEW"))
    if len(project) == 0:
        _log.warning(f'Did not find project {project_name_or_id}')
        return None
    elif len(project) == 1:
        return project[0]
    else:
        raise Exception('Found more than 1 project matching {0}'.format(project_name_or_id))

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)

def mkstempfname(suffix='', prefix='tmp', directory=None, text=False):
    ''' There's no other one-liner way to securely ask for a temp file by
        filename only.  This calls mkstemp, which does what we want, except
        that it returns an open file handle, which causes huge problems on NFS
        if we don't close it.  So close it first then return the name part only.
    '''
    fd, fn = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=directory, text=text)
    os.close(fd)
    return fn

def unlink_tempfile(fname):
    """Unlink the given file if present and if we are not keeping temp files for debug purposes"""
    if os.path.isfile(fname):
        if keep_tmp():
            _log.debug('keeping tempfile %s', fname)
        else:
            os.unlink(fname)

@contextlib.contextmanager
def tempfname(*args, **kwargs):
    '''Create a tempfile name on context entry, delete the file (if it exists) on context exit.
    The file is kept for debugging purposes if the environment variable VIRAL_NGS_TMP_DIRKEEP is set.
    '''
    fn = mkstempfname(*args, **kwargs)
    try:
        yield fn
    finally:
        unlink_tempfile(fn)

@contextlib.contextmanager
def tempfnames(suffixes, *args, **kwargs):
    '''Create a set of tempfile names on context entry, delete the files (if they exist) on context exit.
    The files are kept for debugging purposes if the environment variable VIRAL_NGS_TMP_DIRKEEP is set.
    '''
    fns = [mkstempfname(sfx, *args, **kwargs) for sfx in suffixes]
    try:
        yield fns
    finally:
        for fn in fns:
            unlink_tempfile(fn)

@contextlib.contextmanager
def tmp_dir(*args, **kwargs):
    """Create and return a temporary directory, which is cleaned up on context exit
    unless keep_tmp() is True."""

    _args = inspect.getcallargs(tempfile.mkdtemp, *args, **kwargs)
    length_margin = 6
    for pfx_sfx in ('prefix', 'suffix'):
        if _args[pfx_sfx]:
            _args[pfx_sfx] = string_to_file_name(_args[pfx_sfx], file_system_path=_args['dir'], length_margin=length_margin)
            length_margin += len(_args[pfx_sfx].encode('utf-8'))

    name = None
    try:
        name = tempfile.mkdtemp(**_args)
        yield name
    finally:
        if name is not None:
            if keep_tmp():
                _log.debug('keeping tempdir ' + name)
            else:
                shutil.rmtree(name, ignore_errors=True)

def string_to_file_name(string_value, file_system_path=None, length_margin=0):
    """Constructs a valid file name from a given string, replacing or deleting invalid characters.
    If `file_system_path` is given, makes sure the file name is valid on that file system.
    If `length_margin` is given, ensure a string that long can be added to filename without breaking length limits.
    """
    replacements_dict = {
        "\\": "-", # win directory separator
        "/": "-", # posix directory separator
        os.sep: "-", # directory separator
        "^": "_", # caret
        "&": "_and_", # background
        "\"": "", # double quotes
        r"'": "", # single quotes
        r":": "_", # colon (problem for ntfs)
        r" ": "_", # spaces
        r"|": "-", # shouldn't confuse a vertical bar for a shell pipe
        r"!": ".", # not a bash operator
        r";": ".", # not a terminator
        r"?": "_", # could be mistaken for a wildcard
        r"*": "_", # could be mistaken for a wildcard
        r"`": "_", # no subshells
        r" -": "_-", # could be mistaken for an argument
        r" --": "_--", # could be mistaken for an argument
        r">": "_", # no redirect chars
        r"<": "_", # no redirect chars
        r"(": "__", # special character
        r")": "__", # special character
        r"@": "_", # special character
        r"\\x": "_", # hex char
        r"\\o": "_", # octal char
        #r"\\u": "", # unicode char
        #"": "", # other illegal strings to replace
    }

    # group of ascii control and non-printable characters
    control_chars = ''.join( map(chr, list(range(0,32)) + list(range(127,160)) ) )
    control_char_re = re.compile('[%s]' % re.escape(control_chars))
    string_value = control_char_re.sub("_", string_value)

    # replacements from the dictionary above
    strs_to_replace_re = re.compile(r'|'.join(re.escape(key) for key in replacements_dict.keys()))
    string_value = strs_to_replace_re.sub(lambda x: replacements_dict.get(x.group(), "_"), string_value)

    # condense runs of underscores
    double_underscore_re = re.compile(r'_{2,}')
    string_value = double_underscore_re.sub("_", string_value)

    # condense runs of dashes
    double_dash_re = re.compile(r'-{2,}')
    string_value = double_dash_re.sub("-", string_value)

    # remove leading or trailing periods (no hidden files (*NIX) or missing file extensions (NTFS))
    string_value = string_value.strip(".")

    # comply with file name length limits
    if file_system_path is not None:
        max_len = max(1, max_file_name_length(file_system_path) - length_margin)
        string_value = string_value[:max_len]
        while len(string_value.encode('utf-8')) > max_len:
            string_value = string_value[:-1]

    # ensure all the character removals did not make the name empty
    string_value = string_value or '_'

    return string_value

def _get_pathconf(file_system_path, param_suffix, default):
    """Return a pathconf parameter value for a filesystem.
    """
    param_str = [s for s in os.pathconf_names if s.endswith(param_suffix)]
    if len(param_str) == 1:
        try:
            return os.pathconf(file_system_path, param_str[0])
        except OSError:
            pass
    return default

def max_file_name_length(file_system_path):
    """Return the maximum valid length of a filename (path component) on the given filesystem."""
    return _get_pathconf(file_system_path, '_NAME_MAX', 80)-1

def max_path_length(file_system_path):
    """Return the maximum valid length of a path on the given filesystem."""
    return _get_pathconf(file_system_path, '_PATH_MAX', 255)-1

def keep_tmp():
    """Whether to preserve temporary directories and files (useful during debugging).
    Return True if the environment variable TMP_DIRKEEP is set.
    """
    return 'TMP_DIRKEEP' in os.environ

@contextlib.contextmanager
def create_html_page(html_fname, title=''):
    tags = dominate.tags
    doc = dominate.document(title=title)

    with doc.head:
        tags.meta(charset="UTF-8")
        tags.meta(name="viewport", content="width=device-width, initial-scale=1.0")
        tags.meta(http_equiv="Expires", content="Thu, 1 June 2000 23:59:00 GMT")
        tags.meta(http_equiv="pragma", content="no-cache")
        tags.style('table, th, td {border: 1px solid black;}')
        tags.style('th {background-color: lightblue;}')
        #tags.base(href=os.path.basename(html_fname))

        tags.script(src="http://code.jquery.com/jquery-1.7.1.min.js")

        tags.link(href="https://unpkg.com/tabulator-tables@4.9.3/dist/css/tabulator.min.css", rel="stylesheet")
        tags.script(type="text/javascript", src="https://unpkg.com/tabulator-tables@4.9.3/dist/js/tabulator.min.js")
        
        # tags.link(href="https://raw.githubusercontent.com/olifolkerd/tabulator/4.9.3/dist/css/tabulator.min.css",
        #           rel="stylesheet")
        # tags.script(type='text/javascript',
        #             src="https://raw.githubusercontent.com/olifolkerd/tabulator/4.9.3/dist/js/tabulator.min.js")
        #tags.script(src='https://www.brainbell.com/javascript/download/resizable.js')

    def txt(v): return dominate.util.text(str(v)) if not hasattr(v, 'raw_html') else dominate.util.raw(v.data)
    def trow(vals, td_or_th=tags.td): return tags.tr((td_or_th(txt(val)) for val in vals), __pretty=False)
    raw = dominate.util.raw
    
    def raw_s(v):
        s = collections.UserString(v)
        s.raw_html = True
        return s
    
    with doc:
        raw("""<script  type="text/javascript">
         $(function() {
       $('a[href*="#"]:not([href="#"])').click(function() {
         if (location.pathname.replace(/^\//,'') == this.pathname.replace(/^\//,'') && location.hostname == this.hostname) {
           var target = $(this.hash);
           target = target.length ? target : $('[name=' + this.hash.slice(1) +']');
           if (target.length) {
             $('html, body').animate({
               scrollTop: target.offset().top
             }, 1000);
             return false;
           }
         }
       });
     });
        </script>""")

        tags.div(cls='header').add(txt(datetime.datetime.now()))
        with tags.div(cls='body'):
            tags.h1(title)

        yield (doc, tags, txt, trow, raw, raw_s)

    tags.div(cls='footer').add(txt(datetime.datetime.now()))

    with open(html_fname, 'w') as out:
        out.write(doc.render())

def html_insert_fig(tags):

    my_stringIObytes = io.BytesIO()
    plt.savefig(my_stringIObytes, format='jpg')
    my_stringIObytes.seek(0)
    my_base64_jpgData = base64.b64encode(my_stringIObytes.read())

    encoded = my_base64_jpgData
    #print('type:', type(encoded))
    tags.img(src='data:image/png;base64,'+encoded.decode())

def read_tsvs_stack(tsvs, **read_table_opts):
    """Load a DataFrame from each of the tsvs and stack them vertically"""
    return pd.concat([pd.read_table(tsv, low_memory=False, verbose=True, **read_table_opts) for tsv in tsvs])


