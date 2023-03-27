# DA-1677-italgas-emissions-summary-until-September-30

Steps that are needed to get the emissions summary 

1. Create a data and wind_speed_rotations folders
2. In the data folder place the list of final reports provided by Francesco
4. Check if a wind correction for the current month is needed by checking the dashboard provided by Aaron. You can check the page **"Survey by Analyzer Summary"** and focus on the "Lateral Correlation" column. In general we correct when we have a lateral correlation bigger that 0.3.
5. open the file **"get_emission_sources.py"** file and write down the number of the "current_month = x" and place the current month in the list of wether they need a correction or not. Example: We want to compile the emissions for September, then, current_month = 9 and if September does not need recalculation list_months_no_re_calculation = [6,7,8, **9**].

6. Save and run from terminal (possibly creating an italgas emissions screen, screen -R italgas_em) **"subprocesses.py"**
7. Done. Check especially in the first script **"get_emission_sources.py"** if there are errors that can be due to the list provided.