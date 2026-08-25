# Free-Tier Cloud Deployment Plan

Project: Financial Analytics and Fraud Detection System  
Date checked: 2026-08-22  
Target thesis presentation: 2027-04-27, based on the next April 27 after the current date

## Goal

Run and demo the project from cloud infrastructure for the final thesis period, without depending on the student's laptop or desktop hardware.

Primary priorities:

1. Stay inside free-tier or free-trial limits.
2. Keep the system available during the final 3-month period before the thesis presentation.
3. Preserve the current Docker-based architecture where possible.
4. Avoid services that lose data after idle shutdown or short database expiry.
5. Keep a local fallback path for emergency demo recovery.

## Current Project Reality

The repository documentation describes a 6-system enterprise ecosystem:

- SYS-01: Business / predictive dashboard
- SYS-02: Airflow data pipeline orchestration
- SYS-03: Grafana / Prometheus operations center
- SYS-04: MLflow / FastAPI model serving
- SYS-05: MinIO / ClickHouse / PostgreSQL lakehouse portal
- SYS-06: Telegram alerting

The currently implemented and script-started services are mainly:

- SYS-02: Airflow webserver, scheduler, Airflow PostgreSQL metadata database
- SYS-05: MinIO, ClickHouse, PostgreSQL
- Unified static portal: `portal.html`

The existing demo plan is Local All-in-One plus tunnel. That is good for development, but it is not enough for the final thesis goal because the public demo still depends on the local Windows machine staying powered on and connected.

## Recommended Strategy

Use a cloud VM as the main thesis environment.

Recommended primary cloud:

- Google Cloud Free Trial, activated around 2027-01-27
- Reason: Google Cloud currently provides a 90-day trial with $300 credit for new users, which aligns directly with the final 3-month thesis window.

Recommended backup cloud:

- Oracle Cloud Always Free, created and tested early
- Reason: Oracle Always Free can keep running after the trial period, and the Ampere A1 allowance can be enough for a trimmed Docker deployment. Capacity can be unavailable in some regions, so it should be treated as a backup, not the only plan.

Not recommended as the main full-stack host:

- Render Free: useful for one stateless dashboard or API, but free web services spin down when idle and the free PostgreSQL database expires after 30 days.
- Railway Free Trial: useful for quick testing, but the trial is short and credit is small for a multi-container data platform.
- Fly.io Free Trial: not suitable for a 3-month thesis environment because the trial is very short for new users.
- Koyeb Free: useful for a small single web service, but the free instance is too small for Airflow, ClickHouse, and MinIO together.

## Timing Plan

If the presentation is on 2027-04-27:

- 2026-08 to 2026-12: continue local development and reduce cloud resource requirements.
- 2026-12: prepare Docker Compose profiles, secrets, backup scripts, and cloud deployment notes.
- 2027-01-10 to 2027-01-26: rehearse deployment on a temporary VM or Oracle Always Free.
- 2027-01-27: activate the 90-day Google Cloud trial if not already used.
- 2027-01-27 to 2027-04-20: run the cloud thesis environment, monitor cost daily/weekly.
- 2027-04-21 to 2027-04-27: freeze changes except critical fixes; keep local and Oracle backup ready.

If the Google Cloud trial is activated on 2026-08-22, it would end around 2026-11-20, which is too early for an April 27, 2027 presentation.

## Target Cloud Architecture

Phase 1: Single-VM cloud deployment

Run the same Docker-based stack on one Linux VM:

- Docker Engine and Docker Compose
- SYS-05 storage services first: MinIO, ClickHouse, PostgreSQL
- SYS-02 orchestration services second: Airflow PostgreSQL, Airflow init, webserver, scheduler
- Optional public access by Cloudflare Tunnel, reverse proxy, or VM firewall rules

This is the best match for the current repo because the system already uses Docker Compose and host-mounted volumes.

Phase 2: Split lightweight public services if needed

Only after the single VM works:

- Put a simple dashboard or FastAPI demo on Render/Koyeb/Cloud Run if needed.
- Keep stateful services on the VM or managed storage.
- Do not move ClickHouse/MinIO/Postgres to a free stateless platform.

## Minimum Cloud VM Shape

For full SYS-02 + SYS-05 demo:

- Recommended: 2 vCPU, 8 GB RAM, 50-100 GB disk
- Safer: 4 vCPU, 16 GB RAM, 100 GB disk if trial credit allows
- Operating system: Ubuntu LTS
- CPU architecture: x86_64 is easiest; ARM is possible but must verify every Docker image supports ARM64

For Oracle Always Free backup:

- Use Ampere A1 ARM shape if available.
- Keep total Always Free A1 resources within Oracle's Always Free limit.
- Test Docker images on ARM64 before relying on it for the thesis demo.

## Required Project Improvements

1. Add a root-level cloud compose file.

   Current compose files are split between `SYS-02` and `SYS-05`. A root `docker-compose.cloud.yml` should define a shared Docker network and consistent service names so Airflow can reach MinIO and ClickHouse by container DNS names instead of `host.docker.internal`.

2. Add Docker Compose profiles.

   Suggested profiles:

   - `core`: PostgreSQL, MinIO, ClickHouse
   - `orchestration`: Airflow webserver and scheduler
   - `demo`: portal, dashboard/API if implemented
   - `monitoring`: Prometheus and Grafana if implemented

3. Move credentials to `.env.example`.

   The docs currently expose simple demo credentials such as `admin123`. Keep them for local demo only, but make cloud deployment use environment variables and stronger passwords.

4. Add backup and restore scripts.

   Minimum backups:

   - PostgreSQL dump
   - ClickHouse table export or volume snapshot
   - MinIO bucket mirror/export
   - Airflow DAGs and environment file backup

5. Add cloud health check script.

   Extend `check_health.ps1` or add a Linux `check_health.sh` that checks:

   - Airflow `/health`
   - MinIO console/API
   - ClickHouse `/ping`
   - PostgreSQL port
   - disk usage
   - Docker container status

6. Fix documentation mismatch.

   The master docs list SYS-01, SYS-03, SYS-04, and SYS-06, but the current startup script only starts them if those folders later contain compose files. The thesis plan should label these as planned/optional until implemented.

7. Add a cost guardrail checklist.

   For every cloud:

   - Set budget alerts.
   - Use one region only.
   - Avoid GPUs.
   - Avoid managed load balancers unless included/free.
   - Stop/delete unused disks, snapshots, public IPs, and databases.
   - Keep daily data retention small during demo.

## Final Recommendation

Use this order:

1. Local Docker development now.
2. Oracle Always Free early as a backup rehearsal environment.
3. Google Cloud Free Trial from around 2027-01-27 for the real 3-month thesis run.
4. Render/Koyeb only for small stateless web demos, not for the full data platform.

This is clearer and safer than the current Local All-in-One plan because the final demo no longer depends on personal hardware, and the 90-day trial window is aligned with the April 27 presentation date.

## Sources Checked

- Google Cloud Free Trial: https://cloud.google.com/signup-faqs
- Google Cloud Free Program: https://docs.cloud.google.com/free/docs/free-cloud-features
- Oracle Cloud Free Tier: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier.htm
- Oracle Always Free Resources: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
- AWS EC2 Free Tier documentation: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-free-tier-usage.html
- Azure free account cost guidance: https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/avoid-charges-free-account
- Render Free docs: https://render.com/docs/free
- Railway Free Trial docs: https://docs.railway.com/pricing/free-trial
- Fly.io Free Trial and Pricing docs: https://fly.io/docs/about/free-trial/ and https://www.fly.io/docs/about/pricing/
- Koyeb Free Instance docs: https://www.koyeb.com/docs/reference/instances
