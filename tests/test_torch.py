import logging
import os

import pytest

from cascade.executors.vertex import (
    GcpAcceleratorConfig,
    GcpEnvironmentConfig,
    GcpMachineConfig,
    GcpResource,
)
from cascade.executors.vertex.distributed.torch_job import TorchJob
from cascade.remote import remote

torch = pytest.importorskip("torch")

from torch import nn, optim  # noqa
import torch.nn.functional as F  # noqa
from torch.nn.parallel import DistributedDataParallel as DDP  # noqa
from torch.utils.data.distributed import DistributedSampler  # noqa
from torchvision import datasets, transforms  # noqa


# ====================== Setup test Pytorch model to train ======================


class ConvNet(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 16, kernel_size=3, padding=1)
        self.pool = nn.MaxPool2d(kernel_size=2, stride=2)
        self.conv2 = nn.Conv2d(16, 32, kernel_size=3, padding=1)
        self.fc1 = nn.Linear(32 * 7 * 7, 64)
        self.dropout = nn.Dropout(p=0.5)
        self.fc2 = nn.Linear(64, 10)

    def forward(self, x):
        x = nn.functional.relu(self.conv1(x))
        x = self.pool(x)
        x = nn.functional.relu(self.conv2(x))
        x = self.pool(x)
        x = x.view(-1, 32 * 7 * 7)
        x = nn.functional.relu(self.fc1(x))
        x = self.dropout(x)
        x = nn.functional.softmax(self.fc2(x), dim=1)
        return x


class Trainer:
    def __init__(
        self,
        model: torch.nn.Module,
        train_data: torch.utils.data.DataLoader,
        optimizer: torch.optim.Optimizer,
    ) -> None:
        self.local_rank = int(os.environ["LOCAL_RANK"])
        self.global_rank = int(os.environ["RANK"])
        self.model = model.to(self.local_rank)
        self.train_data = train_data
        self.optimizer = optimizer
        self.epochs_run = 0
        self.model = DDP(self.model, device_ids=[self.local_rank])

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad()
        output = self.model(source)
        loss = F.cross_entropy(output, targets)
        loss.backward()
        self.optimizer.step()

    def _run_epoch(self, epoch):
        b_sz = len(next(iter(self.train_data))[0])
        logging.info(
            f"[GPU{self.global_rank}] Epoch {epoch} | Batchsize: {b_sz} | Steps: {len(self.train_data)}"  # noqa: E501
        )
        for source, targets in self.train_data:
            source = source.to(self.local_rank)  # noqa: PLW2901
            targets = targets.to(self.local_rank)  # noqa: PLW2901
            self._run_batch(source, targets)

    def train(self, max_epochs: int):
        for epoch in range(self.epochs_run, max_epochs):
            self._run_epoch(epoch)
            logging.info(
                f"Epoch {epoch} done on worker [{self.global_rank}/{self.local_rank}]"
            )

        snapshot = {}
        snapshot["MODEL_STATE"] = self.model.module.state_dict()
        snapshot["EPOCHS_RUN"] = epoch
        return snapshot


def load_train_objs():
    train_set = datasets.MNIST(
        root="/app/data/", train=True, transform=transforms.ToTensor(), download=True
    )

    model = ConvNet().cuda()
    optimizer = optim.Adam(model.parameters())
    return train_set, model, optimizer


def prepare_dataloader(dataset: torch.utils.data.Dataset, batch_size: int):
    return torch.utils.data.DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=True,
        shuffle=False,
        sampler=DistributedSampler(dataset),
    )


# ====================== Code to run the test ======================


@pytest.mark.skipif(
    "KOCHIKU_ENV" in os.environ or "BUILDKITE_PIPELINE" in os.environ,
    reason="Run integration tests locally only",
)
def test_torchjob():
    """
    Test launching a Pytorch training job on Vertex
    """
    ACCEL_MACHINE_TYPE = "NVIDIA_TESLA_V100"  # noqa: N806
    WORKER_COUNT = 2  # noqa: N806
    accelerator_config = GcpAcceleratorConfig(count=2, type=ACCEL_MACHINE_TYPE)

    environment = GcpEnvironmentConfig(
        project="ds-cash-production",
        service_account="ds-cash-production@ds-cash-production.iam.gserviceaccount.com",
        region="us-west1",
        network="projects/603986066384/global/networks/neteng-shared-vpc-prod",
        image="us.gcr.io/ds-cash-production/cascade/cascade-test",
    )

    resource = GcpResource(
        chief=GcpMachineConfig(type="n1-standard-16", accelerator=accelerator_config),
        workers=GcpMachineConfig(
            type="n1-standard-16", count=WORKER_COUNT, accelerator=accelerator_config
        ),
        environment=environment,
        distributed_job=TorchJob(),
    )

    @remote(resource=resource, job_name="torchjob-cascade-test")
    def training_task():
        total_epochs = 10

        dataset, model, optimizer = load_train_objs()
        train_data = prepare_dataloader(dataset, batch_size=64)
        trainer = Trainer(model, train_data, optimizer)
        result = trainer.train(total_epochs)

        return result

    result_dict = training_task()
    logging.info(f"Result: {result_dict}")

    # Task/Executor run is expected to return a dictionary containing at the bare
    # minimum a Pytorch state_dict describing a Pytorch model. This dictionary
    # should be directly accessible from the executor.run-returned object, i.e. it
    # does not need to be unpickled or loaded from a remote filestore
    assert "MODEL_STATE" in result_dict
