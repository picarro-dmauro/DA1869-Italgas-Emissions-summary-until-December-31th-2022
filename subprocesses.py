import subprocess

program_list = ['get_emission_sources.py','correct-lateral-wind-speed.py', 're-calculate-emission-rates.py', 'prepare-leaks-with-emission-sources.py' ,'determine-emission-factors.py' ] #'get_emission_sources.py', 'get_emission_sources.py','correct-lateral-wind-speed.py', 

for program in program_list:
    subprocess.call(['python', program])
    print("Finished:" + program)