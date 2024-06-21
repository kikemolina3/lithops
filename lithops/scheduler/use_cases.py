first_stages = [
    {
        'memory': 2048,
        'num_fn': 2,
        'duration': 20
    }
]

hello_stages = [
    {
        'memory': 512,
        'num_fn': 8,
        'duration': 100
    },
    {
        'memory': 512,
        'num_fn': 40,
        'duration': 120
    },
    {
        'memory': 512,
        'num_fn': 16,
        'duration': 30
    }
]

elastic_stages = [
    {
        'num_fn': 100,
        'duration': 45
    },
    {
        'num_fn': 1000,
        'duration': 10
    },
    {
        'num_fn': 500,
        'duration': 20
    },
    {
        'num_fn': 300,
        'duration': 10
    },
    {
        'num_fn': 600,
        'duration': 100
    },
    {
        'num_fn': 1000,
        'duration': 30
    },
    {
        'num_fn': 100,
        'duration': 55
    },
    {
        'num_fn': 100,
        'duration': 15
    },

]

extreme_stages = [
    {
        'num_fn': 200,
        'duration': 10
    },
    {
        'num_fn': 800,
        'duration': 20
    },
    {
        'num_fn': 500,
        'duration': 5
    },
    {
        'num_fn': 300,
        'duration': 10
    },
    {
        'num_fn': 600,
        'duration': 70
    },
    {
        'num_fn': 1000,
        'duration': 5
    },
    {
        'num_fn': 250,
        'duration': 30
    },
    {
        'num_fn': 1200,
        'duration': 15
    },
    {
        'num_fn': 600,
        'duration': 25
    },
    {
        'num_fn': 100,
        'duration': 65
    },
    {
        'num_fn': 250,
        'duration': 10
    },
    {
        'num_fn': 500,
        'duration': 15
    },

]

example_stages = [
    {
        'num_fn': 80,
        'duration': 50
    },
    {
        'num_fn': 200,
        'duration': 25
    },
    {
        'num_fn': 10,
        'duration': 30
    },
    {
        'num_fn': 60,
        'duration': 150
    },
    {
        'num_fn': 120,
        'duration': 50
    },
    {
        'num_fn': 20,
        'duration': 30
    },
]

variant_calling_stages = [
    {
        'input_data': 750,
        'num_fn': 40,
        # 'num_fn': 60,
        'duration': 50,
        'memory': 204.8,
    },
    {
        'num_fn': 200,
        'duration': 55,
        'memory': 204.8,
    },
    {
        'num_fn': 20,
        'duration': 10,
        'memory': 204.8,
    },
    {
        'num_fn': 200,
        'duration': 25,
        'memory': 204.8,
    },
    {
        'num_fn': 40,
        'duration': 40,
        'memory': 204.8,
    },
    {
        'num_fn': 120,
        'duration': 100,
        'memory': 204.8,
    }
]

# TERASORT SETUP
# - 2 EC2 c7i.2xlarge: 8 cores, 16GB RAM
# - worker_processes = 96
terasort_stages = [
    {
        'input_data': 1024 * 100,  # 100GB
        'num_fn': 192,
        'memory': 170.66,
        'duration': 60
    },
    {
        'num_fn': 192,
        'memory': 170.66,
        'duration': 45
    },
]

astronomics_stages = [
    {
        'input_data': 1024 * 8,  # 8GB
        'num_fn': 300,
        'duration': 300
    },
    {
        'num_fn': 200,
        'duration': 250
    },
    {
        'num_fn': 1,
        'duration': 120
    }
]

# all previous stages to a list
stages_list = [
    first_stages,
    hello_stages,
    elastic_stages,
    extreme_stages,
    variant_calling_stages,
    terasort_stages,
    astronomics_stages
]
