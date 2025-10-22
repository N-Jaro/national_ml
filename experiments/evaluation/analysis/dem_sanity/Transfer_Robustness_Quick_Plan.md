# Transfer/Robustness Analysis - Quick Implementation Plan

**Analysis Date:** October 11, 2025  
**Timeline:** Very quick (~1-2 days max)  
**Focus:** Practical insights with minimal implementation overhead  

## 📊 **Terrain Relief Results**

Based on the quick analysis of our 10 representative HUCs:

### **Percentile-Based Thresholds (from 383 patches)**
- **Low-relief threshold:** <35.0m (33rd percentile)
- **High-relief threshold:** >113.9m (67th percentile)  
- **Median relief:** 68.2m

### **HUC Terrain Classification**
- **Low-relief HUCs (2)**: 08050002, 04050001
- **Medium-relief HUCs (3)**: 14080106, 17020006, 12060201
- **High-relief HUCs (5)**: 01030003, 02080201, 18090208, 15030104, 19020103

### **Perfect for Transfer Analysis!**
- ✅ Good distribution: 2 low, 3 medium, 5 high-relief HUCs
- ✅ Clear terrain contrasts (7.8m vs 804.3m mean relief)
- ✅ Diverse geographic representation maintained

---

## 🚀 **Quick Implementation Strategy**

### **Phase 1: Infrastructure (30 minutes)**
```python
# Simple terrain classification
def classify_terrain_bin(dem_patch):
    relief = np.max(dem_patch) - np.min(dem_patch)
    if relief < 35.0:
        return "low_relief"
    elif relief > 113.9:
        return "high_relief" 
    else:
        return "medium_relief"  # We'll group with low for binary analysis

# Simple cloud masking  
def apply_partial_cloud_mask(optical_bands, cloud_coverage=0.3):
    mask = generate_random_cloud_mask(optical_bands.shape, coverage=cloud_coverage)
    return optical_bands * (1 - mask)  # 0 where clouds, original where clear
```

### **Phase 2: 4a Transfer Analysis (1 hour)**
**Models to test:**
1. **AlphaEarth-only**: `/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251003_162616_run10/checkpoints/mdmt-alphaearth-only-epoch=29-val_loss=0.8005.ckpt`
2. **DEM+AlphaEarth**: `/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251004_112528_run2/checkpoints/mdmt-dem-alphaearth-epoch=37-val_loss=0.4154.ckpt`

**Expected results:**
- DEM should provide **greater benefit in high-relief** terrain (19020103, 02080201)
- AlphaEarth-only should perform **better in low-relief** terrain (08050002, 04050001)

### **Phase 3: 4b Robustness Analysis (1 hour)**
**Models that include optical:**
- Need to identify which models have optical components
- Apply 30% cloud coverage simulation
- Measure performance delta (normal vs masked)

**Question:** Do we have any trained models with optical components? Or should we focus on a different robustness test?

### **Phase 4: Quick Visualization (30 minutes)**
```python
# 2x2 mini-bars as requested
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 8))

# Top row: Transfer analysis
ax1.bar(["AlphaEarth-only"], [low_relief_ae_iou], label="Low Relief")
ax1.bar(["DEM+AlphaEarth"], [low_relief_dem_ae_iou])

ax2.bar(["AlphaEarth-only"], [high_relief_ae_iou], label="High Relief") 
ax2.bar(["DEM+AlphaEarth"], [high_relief_dem_ae_iou])

# Bottom row: Robustness analysis  
ax3.bar(["Normal"], [normal_iou], label="No Clouds")
ax3.bar(["30% Clouds"], [cloudy_iou])

ax4.bar(["Performance Delta"], [delta_iou], label="Robustness Loss")
```

---

## 🤔 **Key Questions for Quick Implementation**

### **1. Optical Component Models**
- **Question**: Which trained models include optical bands? 
- **Options**: 
  - All Modalities model (if available)
  - DEM+Optical+AlphaEarth (if exists)
  - Or test different robustness (e.g., SAR masking?)

### **2. Binary vs Ternary Terrain Classification**
- **Current**: Low (2 HUCs), Medium (3 HUCs), High (5 HUCs)
- **Simplified**: Low+Medium (5 HUCs) vs High (5 HUCs) for cleaner comparison?

### **3. Cloud Simulation Approach**
- **30% coverage** as suggested
- **Random patches** or **realistic cloud patterns**?
- **Complete masking** (set to 0) or **fill with mean values**?

### **4. Metrics Priority**
- **mIoU and ClDice** as requested
- **Water segmentation only** or include D8 flow direction?

---

## 📝 **Recommended Next Steps**

### **Immediate (Today)**
1. **Check available model checkpoints** with optical components
2. **Decide on binary terrain classification** (low+medium vs high)
3. **Choose cloud simulation strategy** (30% random masking)

### **Implementation (Tomorrow)**
1. **Copy DEM sanity analysis framework** as baseline
2. **Add terrain classification logic**
3. **Add optical masking logic** 
4. **Run analysis on 10 HUCs** (~30 minutes runtime)
5. **Generate 2x2 visualization**

### **Quick Validation**
- **Sanity check**: DEM benefit should be higher in high-relief terrain
- **Expected pattern**: AlphaEarth-only degrades more in high-relief areas
- **Robustness**: Performance drops with cloud masking, but gracefully

---

## 💡 **Efficiency Hacks for Speed**

### **Code Reuse**
- **90% reuse** from DEM sanity analysis framework
- **Same evaluation pipeline**, just add terrain binning
- **Same HUCs**, same checkpoints where possible

### **Limited Scope**
- **Binary terrain classification** only (low+medium vs high)
- **Single cloud coverage level** (30%)
- **Core metrics only** (IoU, Dice)
- **Same 10 HUCs** (no new data requirements)

### **Quick Validation**
- **Test on 1-2 HUCs first** to validate pipeline
- **Parallel analysis** (terrain + robustness simultaneously)
- **Minimal visualization** (clean but simple)

Would you like me to proceed with this quick implementation plan? The main question is: **do we have trained models with optical components** for the robustness analysis?