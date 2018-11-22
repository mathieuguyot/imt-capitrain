# imt-capitrain
Capitrain project of Quentin EUDE and Mathieu GUYOT

## Run tests
cd imt-capitrain
python -B -m unittest discover --pattern=*Test.py

## Run simulation
cd imt-capitrain
Batsim :
batsim -p platforms//proto2.xml  --config-file config_submit.txt --allow-time-sharing
Pybatsim :
YOUR_PYBATSIM_LOCATION/launcher.py scheduler/src/StorageSched.py