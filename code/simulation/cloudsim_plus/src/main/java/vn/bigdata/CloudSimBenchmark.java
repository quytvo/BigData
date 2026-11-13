package vn.bigdata;

import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudsimplus.utilizationmodels.UtilizationModelFull;
import org.cloudsimplus.vms.VmSimple;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Executes measured work as CloudSim Plus VMs and Cloudlets. */
public final class CloudSimBenchmark {
    private static final double MIPS_PER_CORE = 1000.0;

    private record Work(String scenario, int vmCount, int cores, long ramMb,
                        double bootSec, String stage, int taskId, int vmId,
                        long lengthMi, double submitSec, double transferSec) {
        static Work parse(String line) {
            String[] f = line.split(",", -1);
            if (f.length != 11) throw new IllegalArgumentException("Invalid plan row: " + line);
            return new Work(f[0], Integer.parseInt(f[1]), Integer.parseInt(f[2]),
                    Long.parseLong(f[3]), Double.parseDouble(f[4]), f[5],
                    Integer.parseInt(f[6]), Integer.parseInt(f[7]),
                    Long.parseLong(f[8]), Double.parseDouble(f[9]), Double.parseDouble(f[10]));
        }
    }

    private CloudSimBenchmark() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: CloudSimBenchmark PLAN.csv EVENTS.csv");
        List<String> lines = Files.readAllLines(Path.of(args[0]), StandardCharsets.UTF_8);
        Map<String, List<Work>> scenarios = new LinkedHashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            if (!lines.get(i).isBlank()) {
                Work work = Work.parse(lines.get(i));
                scenarios.computeIfAbsent(work.scenario(), ignored -> new ArrayList<>()).add(work);
            }
        }
        if (scenarios.size() != 9) throw new IllegalArgumentException("Expected nine scale/config scenarios");
        Path output = Path.of(args[1]);
        Files.createDirectories(output.toAbsolutePath().getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("scenario,task_stage,task_id,vm_id,start_sec,finish_sec,transfer_sec,vm_start_sec,vm_finish_sec\n");
            for (var entry : scenarios.entrySet()) runScenario(entry.getKey(), entry.getValue(), writer);
        }
        System.out.println("CloudSim Plus completed " + scenarios.size() + " scenarios: " + output);
    }

    private static void runScenario(String name, List<Work> work, BufferedWriter writer) throws Exception {
        Work first = work.getFirst();
        CloudSimPlus simulation = new CloudSimPlus();
        List<HostSimple> hosts = new ArrayList<>();
        List<VmSimple> vms = new ArrayList<>();
        for (int i = 0; i < first.vmCount(); i++) {
            List<Pe> pes = new ArrayList<>();
            for (int core = 0; core < first.cores(); core++) pes.add(new PeSimple(MIPS_PER_CORE));
            hosts.add(new HostSimple(first.ramMb(), 10000, 100000, pes));
            VmSimple vm = new VmSimple(MIPS_PER_CORE, first.cores());
            vm.setRam(first.ramMb()).setBw(1000).setSize(10000);
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());
            vm.setSubmissionDelay(first.bootSec());
            vms.add(vm);
        }
        new DatacenterSimple(simulation, hosts);
        DatacenterBrokerSimple broker = new DatacenterBrokerSimple(simulation);
        broker.submitVmList(vms);
        List<CloudletSimple> cloudlets = new ArrayList<>();
        for (Work task : work) {
            if (task.vmId() < 0 || task.vmId() >= first.vmCount())
                throw new IllegalArgumentException("Invalid VM for " + name);
            CloudletSimple cloudlet = new CloudletSimple(task.lengthMi(), first.cores(), new UtilizationModelFull());
            cloudlet.setSubmissionDelay(task.submitSec());
            cloudlet.setVm(vms.get(task.vmId()));
            cloudlets.add(cloudlet);
        }
        broker.submitCloudletList(cloudlets);
        simulation.start();
        if (broker.getCloudletFinishedList().size() != work.size())
            throw new IllegalStateException("CloudSim Plus did not finish every Cloudlet in " + name);
        for (int i = 0; i < work.size(); i++) {
            Work task = work.get(i);
            CloudletSimple cloudlet = cloudlets.get(i);
            writer.write(String.format(java.util.Locale.ROOT, "%s,%s,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f%n",
                    name, task.stage(), task.taskId(), task.vmId(), cloudlet.getStartTime(),
                    cloudlet.getFinishTime(), task.transferSec(), vms.get(task.vmId()).getStartTime(),
                    vms.get(task.vmId()).getFinishTime()));
        }
    }
}
