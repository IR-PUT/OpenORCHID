import asyncio
import random
from loguru import logger as log


from src.idmo import IDMO
from src.service_adapter import ServiceAdapter
from src.api import DomainType, UnityRanCapabilities, UnityCoreCapabilities

log.remove()
log.add(
    lambda msg: print(msg, end=''),
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | {level.name} | <cyan>{file}:{line}</cyan> - <level>{message}</level>",
    level="INFO"
)


async def main():

    mode = 3
    workers = []

    if mode == 0:

        idmo = IDMO(
            heartbeat_interval_s=10.0,
            max_missed_heartbeats=3
        )
        workers = [
            asyncio.create_task(idmo.run()),
        ]

    # log.info('Waiting for Serivce adapters to register...')
    # await asyncio.sleep(5.0)

    elif mode == 1:
        domain_a = ServiceAdapter(
            domain_name='my-CORE1',
            domain_type=DomainType.CORE,
            capabilities=UnityCoreCapabilities(
                cpu=8,
                memory_gb=16,
                storage_gb=100,
                maxNumberofUEs=1000
            )
        )
        workers.append(asyncio.create_task(domain_a.run()))
    # await asyncio.sleep(6.0)

    elif mode == 2:
        domain_b = ServiceAdapter(
            domain_name='my-RAN1',
            domain_type=DomainType.RAN,
            capabilities=UnityRanCapabilities(
                maxNumberofUEs=500,
                nROperatingBands=['n78', 'n258'],
                coverageAreaTAList=[(310, 260, 1), (310, 260, 2)],
                positioning=True
            )
        )
        workers.append(asyncio.create_task(domain_b.run()))

    # --- MODE 3: COMPLEX SIMULATION WITH GUARANTEED PROFILES ---
    elif mode == 3:
        log.info("Starting simulation with guaranteed profiles (Urban, Rural, Suburban)...")
        
        # 1. Start IDMO
        idmo = IDMO(heartbeat_interval_s=5.0, max_missed_heartbeats=3)
        workers.append(asyncio.create_task(idmo.run()))
        await asyncio.sleep(1.0)

        # 2. Define Shared Pool of TAs
        shared_ta_pool = [
            (260, 6, 1000), (260, 6, 2000), (260, 6, 3000), (260, 6, 4000)
        ]

        # Define RAN Profiles
        RAN_PROFILES = {
            "URBAN": {"bands": ['n78', 'n258'], "max_ue": 1000, "pos": True},
            "RURAL": {"bands": ['n28', 'n1'], "max_ue": 200, "pos": False},
            "SUBURBAN": {"bands": ['n78', 'n1'], "max_ue": 500, "pos": True}
        }

        num_core = 2
        total_ran_count = 6 # Total number of RANs we want
        
        # 3. Create CORE domains
        core_names = []
        for i in range(num_core):
            c_name = f"CORE-Domain-{i+1}"
            core_names.append(c_name)
            workers.append(asyncio.create_task(ServiceAdapter(
                domain_name=c_name,
                domain_type=DomainType.CORE,
                capabilities=UnityCoreCapabilities(cpu=16, memory_gb=64, storage_gb=500, maxNumberofUEs=5000)
            ).run()))

        # 4. Create RAN domains
        all_profile_names = list(RAN_PROFILES.keys())

        for i in range(total_ran_count):
            # Logic: First 3 RANs get 3 different profiles, others are random
            if i < len(all_profile_names):
                p_name = all_profile_names[i] # Guaranteed: URBAN, then RURAL, then SUBURBAN
            else:
                p_name = random.choice(all_profile_names) # The rest is random
            
            profile = RAN_PROFILES[p_name]

            ran_name = f"RAN-{p_name}-{i+1}"

            mandatory_core = random.choice(core_names)
            selected_cores = {mandatory_core}

            for c_name in core_names:
                if c_name != mandatory_core and random.random() > 0.3: # 70% chance to have a link
                    selected_cores.add(c_name)

            my_links = {}
            for c_name in selected_cores:
                my_links[c_name] = random.uniform(2.0, 50.0)
            
            # Select 2 TAs to ensure overlaps
            assigned_tas = random.sample(shared_ta_pool, k=2)

            ran_adapter = ServiceAdapter(
                domain_name=ran_name,
                domain_type=DomainType.RAN,
                capabilities=UnityRanCapabilities(
                    maxNumberofUEs=profile["max_ue"],
                    nROperatingBands=profile["bands"],
                    coverageAreaTAList=assigned_tas,
                    positioning=profile["pos"],
                    core_links=my_links
                )
            )
            workers.append(asyncio.create_task(ran_adapter.run()))
            log.info(f"Added {ran_name} | Profile: {p_name} | TAs: {[ta[2] for ta in assigned_tas]}")
            await asyncio.sleep(0.2)

    try:
        log.info(f"Simulation running with {len(workers)} active agents.")
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        log.info('Closing...')

    for worker in workers:
        worker.cancel()

    await asyncio.sleep(0.1)

    await asyncio.gather(*workers, return_exceptions=True)




if __name__ == "__main__":
    
    asyncio.run(main(), debug=True)
    
    
