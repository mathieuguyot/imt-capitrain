# imt-capitrain
## 📖 Description
Capitrain project of Quentin EUDE and Mathieu GUYOT.

## 🧰 Run tests
`cd imt-capitrain`
`python -B -m unittest discover --pattern=*Test.py`

## 🔧 Run simulation
- `cd imt-capitrain`
- Batsim : `batsim -p platforms//proto2.xml  --config-file config_submit.txt --allow-time-sharing`
- Pybatsim : `YOUR_PYBATSIM_LOCATION/launcher.py scheduler/src/StorageSched.py`

## 📈 Vizualisation
- Vizualisation example of possible transfer graph by using the [networkX package](https://networkx.github.io/documentation/networkx-1.9/overview.html).
- Easier to find the shortest path and to have an idea of the transfer into the simulated network.
![alt text](https://raw.githubusercontent.com/ouranos588/imt-capitrain/master/Graphdyn!5.png)

## ✔️ TODO
- Improve vizualisation by dividing overlapping edge and by coloring the shortest path to transfer data.
- Run the FW algorithm and after the datasets moves (problem with pybatsim)


