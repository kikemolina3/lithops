first_stages = [
    {
        'num_fn': 4,
        'duration': 300
    }
]

hello_stages = [
    {
        'memory': 2048,
        'num_fn': 2,
        'duration': 100
    },
    {
        'memory': 2048,
        'num_fn': 10,
        'duration': 120
    },
    {
        'memory': 2048,
        'num_fn': 4,
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


variant_calling_stages = [
    {
        'input_data': 750,
        'num_fn': 40,
        'duration': 50,
    },
    {
        'num_fn': 1200,
        'duration': 55,
    },
    {
        'num_fn': 20,
        'duration': 10,
    },
    {
        'num_fn': 1500,
        'duration': 25,
    },
    {
        'num_fn': 40,
        'duration': 40,
    },
    {
        'num_fn': 120,
        'duration': 100,
    }
]

terasort_stages = [
    {
        'input_data': 1024*100, # 100GB
        'num_fn': 192,
        'duration': 75
    },
    {
        'num_fn': 192,
        'duration': 75
    },
]

astronomics_stages = [
    {
        'input_data': 1024*8, # 8GB
        'num_fn': 300,
        'duration': 300
    },
    {
        'num_fn': 150,
        'duration': 250
    },
    {
        'num_fn': 20,
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




