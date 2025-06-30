import torch
import numpy as np
import os
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms
from glob import glob
from tqdm import tqdm


def shape_check(d):
    if d.ndim == 2:
        d = np.expand_dims(d, axis=0)  # (H, W) -> (1, H, W)

    # Dealing with (H, W, C) -> (C, H, W)
    # Channel usually is the smallest  (eg. < 5)
    # Can be dangerous
    if d.ndim == 3 and d.shape[0] > d.shape[2] and d.shape[2] < 5:
        d = np.transpose(d, (2, 0, 1))

    return d

def calculate_mean_std(file_paths, keys=['dem', 'optical', 'thermal', 'sar']):
    sums = {key: 0 for key in keys}
    sum_sqs = {key: 0 for key in keys}
    pixel_counts = {key: 0 for key in keys}
    print("Calculating Overall Mean & Std...")
    for file_path in tqdm(file_paths, desc="Calculating Stats"):
        with np.load(file_path) as data:
            for key in keys:
                d = data[key].astype(np.float32)

                d = shape_check(d)
                
                # optional
                # if key == 'optical':
                #     d = d / 255.0

                sums[key] += np.sum(d, axis=(1, 2))
                sum_sqs[key] += np.sum(d**2, axis=(1, 2))
                pixel_counts[key] += d.shape[1] * d.shape[2]

    total_pixels = {key: pixel_counts[key] for key in keys}
    means = {key: sums[key] / total_pixels[key] for key in keys}
    stds = {key: np.sqrt(sum_sqs[key] / total_pixels[key] - means[key]**2) for key in keys}
    for key in keys:
        stds[key][stds[key] == 0] = 1.0
    return means, stds


class DictTransform:
    def __init__(self, transforms_dict): self.transforms_dict = transforms_dict
    def __call__(self, sample_dict):
        transformed_sample = {}
        for key, tensor in sample_dict.items():
            if key in self.transforms_dict: transformed_sample[key] = self.transforms_dict[key](tensor)
            else: transformed_sample[key] = tensor
        return transformed_sample


class MultiNpzDataset(Dataset):
    def __init__(self, root_dir, transform=None):
        self.file_paths = sorted(glob(os.path.join(root_dir, '*.npz')))
        self.keys = ['dem', 'optical', 'thermal', 'sar']
        self.transform = transform

    def __len__(self):
        return len(self.file_paths)

    def __getitem__(self, idx):
        file_path = self.file_paths[idx]
        with np.load(file_path) as data:
            sample = {}
            # print(data.type())
            for key in self.keys:
                d = data[key]
                # d = shape_check(d)
                 # (H, W) -> (1, H, W)
                sample[key] = d
            
        if self.transform:
            sample = self.transform(sample)

        return sample



# For use this dataset
if __name__ == '__main__':
    DATA_DIR = 'datasets'
    BATCH_SIZE = 16
    KEYS = ['dem', 'optical', 'thermal', 'sar']

    all_files = sorted(glob(os.path.join(DATA_DIR, '*.npz')))
    means, stds = calculate_mean_std(all_files, keys=KEYS)
    
    print("\nThe calculated global statistical data:")
    for key in KEYS:
        print(f"  Key '{key}': Mean={means[key].tolist()}, Std={stds[key].tolist()}")
    

    data_transforms = {
        'dem': transforms.Compose([ 
            transforms.ToTensor(),
            transforms.Normalize(mean=means['dem'], 
                                 std=stds['dem'])]),
        'optical': transforms.Compose([
            transforms.ToTensor(), 
            transforms.Normalize(mean=means['optical'], 
                                 std=stds['optical'])]),
        'thermal': transforms.Compose([     
            transforms.ToTensor(),
            transforms.Normalize(mean=means['thermal'], 
                                  std=stds['thermal'])]),
        'sar': transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize(mean=means['sar'], std=stds['sar'])])
    }

    composed_transform = DictTransform(data_transforms)
    dataset = MultiNpzDataset(root_dir=DATA_DIR, transform=composed_transform)
    dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True, num_workers=0)

    # Write training process here.
    
    print("\nObtain a batch of data from the DataLoader for inspection...")
    data_batch = next(iter(dataloader))
    print("Successfully obtained the data for one batch!")
    for key, tensor in data_batch.items():
        print(f"  Key '{key}': Tensor Shape: {tensor.shape}, Type: {tensor.dtype}")
    print("\nFinished.")