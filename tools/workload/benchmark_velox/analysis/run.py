import fire
import papermill as pm

def exec(inputnb, outputnb, appid, disk, nic, tz, basedir, name, compare_appid='', compare_basedir='', compare_name=''):
  return pm.execute_notebook(
    inputnb,
    outputnb,
    parameters=dict(appid=appid,disk=disk,nic=nic,tz=tz,basedir=basedir,name=name,compare_appid=compare_appid,compare_basedir=compare_basedir,compare_name=compare_name))

if __name__ == '__main__':
  fire.Fire(exec)
