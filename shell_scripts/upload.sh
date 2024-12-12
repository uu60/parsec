#!/bin/zsh

#hosts=("ub1" "ub2" "ub3")
hosts=("du5@ppdsa-a6000.luddy.indiana.edu")
cd "$(dirname $(readlink -f "$0"))"
cd ../
dir_to_upload="$PWD"
cd ../
demo_dir="$PWD/demo"

for host in "${hosts[@]}"; do
    ssh "$host" "rm -rf ~/$(basename "$dir_to_upload") ~/$(basename "$demo_dir")"

    rsync -av "$dir_to_upload" "$host:~/"
    rsync -av "$demo_dir" "$host:~/"
done
echo "DONE"
echo "Please run on the servers: "
echo "'sh ~/$(basename "$dir_to_upload")/shell_scripts/install.sh && sh ~/$(basename "$demo_dir")/build.sh'"
echo "'mpirun -np 3 -hostfile ~/$(basename "$demo_dir")/hostfile.txt ~/$(basename "$demo_dir")/build/demo -case 5'"