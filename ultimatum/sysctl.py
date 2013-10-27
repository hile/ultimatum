"""
Reading and writing of sysctl variables as dictionaries
"""

from subprocess import check_output,CalledProcessError

class SysCtlError(Exception):
    def _str__(self):
        return self.args[0]

class SysCtlTree(dict):
    def __init__(self,path=None):
        cmd = path is not None and ['sysctl','-e',path] or ['sysctl','-ea']
        try:
            output = check_output(cmd)
        except CalledProcessError:
            raise SysCtlError('Error running command %s' % ' '.join(cmd))
        for l in [l.strip() for l in output.split('\n') if l.strip()!='']:
            try:
                k,v = l.split('=',1)
                self[k] = v
            except ValueError:
                raise SysCtlError('Error parsing line: %s' % l)

if __name__ == '__main__':
    import sys
    for t in sys.argv[1:]:
        t = SysCtlTree(t)
        for k,v in t.items():
            print k,v
