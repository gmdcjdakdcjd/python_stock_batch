import tensorflow as tf

print("=== GPU Devices ===")
print(tf.config.list_physical_devices("GPU"))

print("=== Is Built With CUDA? ===")
print(tf.test.is_built_with_cuda())

print("=== GPU Available? ===")
print(tf.config.list_physical_devices())
