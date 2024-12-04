import os
import shutil
import pytest
from pyfakefs.fake_filesystem_unittest import Patcher

@pytest.fixture(autouse=True)
def fs():
    """Create a fake filesystem."""
    patcher = Patcher(
        modules_to_reload=[],
        # Skip patching any torch modules that try to access os
        additional_skip_names=[
            "torch._functorch.config",
            "torch._inductor.config",
            "torch.fx.experimental._config",
            "torch._dynamo.config",
            "torch",
        ]
    )
    patcher.setUp()
    
    # Create the site-packages directory structure first
    site_packages_path = '/Users/angelo/Development/cascade/.venv/lib/python3.9/site-packages'
    os.makedirs(site_packages_path, exist_ok=True)
    
    # Create package metadata for block_cascade
    dist_info_path = os.path.join(site_packages_path, 'block_cascade-2.6.2.dist-info')
    os.makedirs(dist_info_path, exist_ok=True)
    metadata_path = os.path.join(dist_info_path, 'METADATA')
    patcher.fs.create_file(metadata_path, contents="""Metadata-Version: 2.1
Name: block-cascade
Version: 2.6.2
Summary: Block Cascade
Author: Test Author
Author-email: test@example.com
Requires-Python: >=3.9
""")
    
    # Add the torch directory as a real directory
    patcher.fs.add_real_directory(os.path.join(site_packages_path, 'torch'))
    
    # Create temp directory structure
    tmp_path = '/var/folders/lt/6znk4r891xdcrhm2s2cjlzk40000gn/T/pytest-of-angelo'
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)
    os.makedirs(tmp_path, exist_ok=True)
    os.chmod(tmp_path, 0o700)
    
    # Create numbered temp directories
    for i in range(10):
        numbered_dir = os.path.join(tmp_path, f'pytest-{i}')
        os.makedirs(numbered_dir, exist_ok=True)
        os.chmod(numbered_dir, 0o700)
    
    # Create current symlink
    current_link = os.path.join(tmp_path, 'pytest-current')
    if os.path.exists(current_link):
        os.unlink(current_link)
    os.symlink(os.path.join(tmp_path, 'pytest-0'), current_link)
    
    yield patcher.fs
    patcher.tearDown()